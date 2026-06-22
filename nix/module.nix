{
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.evix;
in {
  options.services.evix = {
    enable = lib.mkEnableOption "evix Nix evaluator tools";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.callPackage ./package.nix {};
      defaultText = lib.literalExpression "pkgs.callPackage ./package.nix {}";
      description = "evix package to install.";
    };

    daemon = {
      enable = lib.mkEnableOption "the evix daemon user service";

      socket = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        example = "%t/evix.sock";
        description = "Optional EVIX_SOCKET path for the user daemon.";
      };
    };
  };

  config = lib.mkIf cfg.enable {
    environment.systemPackages = [cfg.package];

    systemd.user.services.evixd = lib.mkIf cfg.daemon.enable {
      description = "evix daemon";
      wantedBy = ["default.target"];
      serviceConfig = {
        ExecStart = "${lib.getExe' cfg.package "evixd"} --foreground";
        Restart = "on-failure";
      };
      environment = lib.optionalAttrs (cfg.daemon.socket != null) {
        EVIX_SOCKET = cfg.daemon.socket;
      };
    };
  };
}
