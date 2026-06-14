{
  lib,
  rustPlatform,
}:
rustPlatform.buildRustPackage (finalAttrs: {
  pname = "nix-eval-jobs-rs";
  version = "0.1.0";

  src = let
    fs = lib.fileset;
    s = ../.;
  in
    fs.toSource {
      root = s;
      fileset = fs.unions [
        (s + /src)
        (s + /Cargo.lock)
        (s + /Cargo.toml)
      ];
    };

  cargoLock.lockFile = "${finalAttrs.src}/Cargo.lock";
  enableParallelBuilding = true;

  meta = {
    description = "Evaluate a Nix expression and stream derivation info as JSON lines";
    mainProgram = "nix-eval-jobs";
    license = lib.licenses.eupl12;
    maintainers = with lib.maintainers; [NotAShelf];
  };
})
