use std::collections::BTreeMap;

use anyhow::{Context as _, Result};
use nix_bindings::{EvalState, Store, StorePath, Value, ValueType};
use tracing::{debug, warn};

use crate::{Config, EvalError, Event};

pub fn process_attr<'s>(
    state: &'s EvalState,
    store: &Store,
    root: &Value<'s>,
    path: &[String],
    auto_args: Option<&Value<'s>>,
    config: &Config,
) -> Event {
    let attr = path.join(".");

    let value = match navigate(state, root, path, auto_args) {
        Ok(v) => v,
        Err(e) => {
            return Event::Error(EvalError {
                attr,
                attr_path: path.to_vec(),
                error: e.to_string(),
                fatal: false,
            });
        }
    };

    if value.value_type() != ValueType::Attrs {
        return Event::AttrSet {
            attr,
            attr_path: path.to_vec(),
            attrs: vec![],
        };
    }

    match state.get_derivation(&value) {
        Ok(Some(drv_path)) => match make_job(store, &value, path, drv_path, config) {
            Ok(ev) => ev,
            Err(e) => Event::Error(EvalError {
                attr,
                attr_path: path.to_vec(),
                error: e.to_string(),
                fatal: false,
            }),
        },
        Ok(None) => {
            let children = collect_recurse(&value, path, config.force_recurse);
            Event::AttrSet {
                attr,
                attr_path: path.to_vec(),
                attrs: children,
            }
        }
        Err(e) => Event::Error(EvalError {
            attr,
            attr_path: path.to_vec(),
            error: e.to_string(),
            fatal: false,
        }),
    }
}

fn navigate<'s>(
    state: &'s EvalState,
    root: &Value<'_>,
    path: &[String],
    auto_args: Option<&Value<'s>>,
) -> Result<Value<'s>> {
    if path.is_empty() {
        return Ok(state.auto_call_function(auto_args, root)?);
    }
    let mut current: Value<'s> = {
        let raw = root.get_attr(&path[0])?;
        state.auto_call_function(auto_args, &raw)?
    };
    for key in &path[1..] {
        let next = {
            let raw = current.get_attr(key)?;
            state.auto_call_function(auto_args, &raw)?
        };
        current = next;
    }
    Ok(current)
}

fn collect_recurse(value: &Value<'_>, path: &[String], force_recurse: bool) -> Vec<String> {
    let Ok(keys) = value.attr_keys() else {
        return vec![];
    };

    let recurse = force_recurse
        || path.is_empty()
        || value
            .get_attr("recurseForDerivations")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

    if recurse {
        keys.into_iter()
            .filter(|k| k != "recurseForDerivations")
            .collect()
    } else {
        vec![]
    }
}

fn make_job(
    store: &Store,
    value: &Value<'_>,
    path: &[String],
    drv_path: nix_bindings::StorePath,
    config: &Config,
) -> Result<Event> {
    let attr = path.join(".");
    let drv_path_str = store.print_path(&drv_path).context("printing drv path")?;

    let name = value
        .get_attr("name")
        .and_then(|v| v.as_string())
        .context("reading .name")?;
    let system = value
        .get_attr("system")
        .and_then(|v| v.as_string())
        .unwrap_or_default();
    let outputs = output_paths(value);

    let meta = if config.meta { read_meta(value) } else { None };
    let constituents = read_constituents(value);
    let input_drvs = if config.show_input_drvs {
        read_input_drvs(store, &drv_path)
    } else {
        BTreeMap::new()
    };

    let gc_root_error = config.gc_roots_dir.as_ref().and_then(|dir| {
        register_gc_root(dir, &drv_path_str).err().map(|e| {
            warn!(drv_path = %drv_path_str, error = %e, "failed to register gc root");
            e.to_string()
        })
    });

    debug!(name = %name, drv_path = %drv_path_str, "found derivation");

    Ok(Event::Derivation(crate::Derivation {
        attr,
        attr_path: path.to_vec(),
        name,
        system,
        drv_path: drv_path_str,
        outputs,
        meta,
        input_drvs,
        constituents,
        gc_root_error,
    }))
}

/// Read a derivation's `meta` attribute as freeform JSON. Best-effort: returns
/// `None` when the attribute is absent. Individual nested attributes that fail
/// to force (functions, throwing values) are skipped rather than discarding the
/// whole `meta` set.
fn read_meta(value: &Value<'_>) -> Option<serde_json::Value> {
    if !value.has_attr("meta").unwrap_or(false) {
        return None;
    }
    let meta = value.get_attr("meta").ok()?;
    value_to_json(meta)
}

/// Recursively convert a forced Nix value into JSON. Each node is forced to
/// weak-head normal form on entry; nodes that fail to force, and value kinds
/// without a JSON analogue (functions, external values), yield `None` so the
/// caller can skip them.
fn value_to_json(mut value: Value<'_>) -> Option<serde_json::Value> {
    use serde_json::Value as J;

    value.force().ok()?;
    match value.value_type() {
        ValueType::Null => Some(J::Null),
        ValueType::Bool => value.as_bool().ok().map(J::Bool),
        ValueType::Int => value.as_int().ok().map(|i| J::Number(i.into())),
        ValueType::Float => value
            .as_float()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(J::Number),
        ValueType::String => value.as_string().ok().map(J::String),
        ValueType::Path => value
            .as_path()
            .ok()
            .map(|p| J::String(p.to_string_lossy().into_owned())),
        ValueType::List => {
            let len = value.list_len().ok()?;
            let mut arr = Vec::with_capacity(len);
            for i in 0..len {
                let item = value.list_get(i).ok()?;
                arr.push(value_to_json(item).unwrap_or(J::Null));
            }
            Some(J::Array(arr))
        }
        ValueType::Attrs => {
            let keys = value.attr_keys().ok()?;
            let mut map = serde_json::Map::new();
            for key in keys {
                if let Ok(child) = value.get_attr(&key)
                    && let Some(child_json) = value_to_json(child)
                {
                    map.insert(key, child_json);
                }
            }
            Some(J::Object(map))
        }
        ValueType::Thunk | ValueType::Function | ValueType::External => None,
    }
}

/// Read the `constituents` attribute of an aggregate (Hydra) job as a list of
/// attribute-path strings. Returns `None` for ordinary derivations that do not
/// declare the attribute.
fn read_constituents(value: &Value<'_>) -> Option<Vec<String>> {
    if !value.has_attr("constituents").unwrap_or(false) {
        return None;
    }
    let mut list = value.get_attr("constituents").ok()?;
    list.force().ok()?;
    let len = list.list_len().ok()?;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if let Ok(item) = list.list_get(i)
            && let Ok(s) = item.as_string()
        {
            out.push(s);
        }
    }
    Some(out)
}

/// Read a derivation's input derivations by parsing the `.drv` file's JSON
/// representation. Keyed by input `.drv` store path. Best-effort: returns an
/// empty map when the derivation cannot be read or serialized.
fn read_input_drvs(store: &Store, drv_path: &StorePath) -> BTreeMap<String, serde_json::Value> {
    let mut map = BTreeMap::new();
    let Ok(drv) = store.read_derivation(drv_path) else {
        return map;
    };
    let Ok(json) = drv.to_json() else {
        return map;
    };
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&json) else {
        return map;
    };
    // `nix_derivation_to_json` nests input derivations under `inputs.drvs` and
    // keys them by store-relative basename. Re-add the store prefix so keys are
    // absolute `.drv` paths, and expose the value as the output-name list to
    // match the `nix-eval-jobs` `inputDrvs` contract (`{drv: ["out", ...]}`).
    let store_dir = store
        .store_dir()
        .unwrap_or_else(|_| "/nix/store".to_string());
    let Some(drvs) = parsed
        .get("inputs")
        .and_then(|inputs| inputs.get("drvs"))
        .and_then(serde_json::Value::as_object)
    else {
        return map;
    };
    for (key, value) in drvs {
        let full_path = if key.starts_with('/') {
            key.clone()
        } else {
            format!("{store_dir}/{key}")
        };
        let outputs = value
            .get("outputs")
            .cloned()
            .unwrap_or_else(|| value.clone());
        map.insert(full_path, outputs);
    }
    map
}

fn output_paths(value: &Value<'_>) -> BTreeMap<String, Option<String>> {
    let mut map = BTreeMap::new();
    let Ok(list) = value.get_attr("outputs") else {
        return map;
    };
    let Ok(len) = list.list_len() else {
        return map;
    };
    for i in 0..len {
        let Ok(name_val) = list.list_get(i) else {
            continue;
        };
        let Ok(name) = name_val.as_string() else {
            continue;
        };
        let path = output_path_for(value, &name);
        map.insert(name, path);
    }
    map
}

/// Resolve the store path of a single output. Each output is exposed on the
/// derivation as an attribute whose `outPath` is the store path; fall back to
/// coercing the attribute directly for non-standard derivations.
fn output_path_for(value: &Value<'_>, name: &str) -> Option<String> {
    let out = value.get_attr(name).ok()?;
    if let Ok(path) = out.get_attr("outPath").and_then(|v| v.as_string()) {
        return Some(path);
    }
    if let Ok(s) = out.as_string() {
        return Some(s);
    }
    out.as_path().ok().map(|p| p.to_string_lossy().into_owned())
}

fn register_gc_root(gc_dir: &std::path::Path, drv_path: &str) -> Result<()> {
    let name = std::path::Path::new(drv_path)
        .file_name()
        .context("drv path has no filename")?;
    let link = gc_dir.join(name);
    if !link.exists() {
        std::os::unix::fs::symlink(drv_path, &link)
            .with_context(|| format!("symlinking {link:?} -> {drv_path}"))?;
    }
    Ok(())
}
