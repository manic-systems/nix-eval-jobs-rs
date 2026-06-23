use std::{env, fs, path::PathBuf};

fn main() {
  println!("cargo:rerun-if-changed=schema/worker.capnp");

  // docs.rs has no Nix system libraries. Write empty bindings so the crate
  // compiles and return before any pkg-config or cc invocation.
  if env::var("DOCS_RS").is_ok() {
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    fs::write(out_path.join("bindings.rs"), "")
      .expect("write stub bindings for docs.rs");
    return;
  }

  capnpc::CompilerCommand::new()
    .src_prefix("schema")
    .file("schema/worker.capnp")
    .run()
    .expect("compile worker Cap'n Proto schema");
}
