{
  mkShell,
  rustPlatform,
  rustc,
  cargo,
  rust-analyzer,
  rustfmt,
  clippy,
  taplo,
  pkg-config,
  nixVersions,
  glibc,
}:
let
  inherit (rustc) llvmPackages;
  nixForBindings = nixVersions.nix_2_34;
in
mkShell {
  name = "nix-eval-jobs";

  strictDeps = true;
  nativeBuildInputs = [
    pkg-config
    cargo
    rustc
    llvmPackages.lld
    rustfmt
    clippy
    taplo
    rust-analyzer
  ];

  buildInputs = [
    nixForBindings.dev
    glibc.dev
  ];

  env = {
    RUST_SRC_PATH = "${rustPlatform.rustLibSrc}";
    LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
    BINDGEN_EXTRA_CLANG_ARGS = "--sysroot=${glibc.dev}";
  };
}
