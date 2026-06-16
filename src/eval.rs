use std::collections::BTreeMap;

use anyhow::{Context as _, Result};
use nix_bindings::{EvalState, Store, Value, ValueType};
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
        gc_root_error,
    }))
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
        let path = value.get_attr(&name).ok().and_then(|out| {
            out.as_string()
                .ok()
                .or_else(|| out.as_path().map(|p| p.to_string_lossy().into_owned()).ok())
        });
        map.insert(name, path);
    }
    map
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
