self:
{
  config,
  pkgs,
  lib,
  ...
}:
let
  cfg = config.services.rift;
  rift = "${self.packages.${pkgs.stdenv.hostPlatform.system}.rift}";
in
{
  imports = [
    (import ./snapshots self)
    (import ./prune self)
    (import ./sync self)
  ];

  options.services.rift = {
    enable = lib.mkEnableOption "rift zfs services";
  };

  config = lib.mkIf cfg.enable {
    environment.systemPackages = [ rift ];
  };
}
