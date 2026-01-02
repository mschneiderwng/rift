{
  config,
  pkgs,
  lib,
  ...
}:
let
  mkPermissions =
    action: user: permissions: dataset:
    lib.escapeShellArgs [
      "-+/run/booted-system/sw/bin/zfs"
      action
      user
      (lib.concatStringsSep "," permissions)
      dataset
    ];
in
{
  allow =
    user: perm: datasets:
    (map (mkPermissions "allow" user perm) datasets);

  unallow =
    user: permissions: datasets:
    (map (mkPermissions "unallow" user permissions) datasets);

  attrKeys = attrs: lib.mapAttrsToList (name: value: name) attrs;

  # Escape as required by: https://www.freedesktop.org/software/systemd/man/systemd.unit.html
  escapeUnitName =
    name:
    lib.concatMapStrings (s: if lib.isList s then "-" else s) (
      builtins.split "[^a-zA-Z0-9_.\\-]+" name
    );
}
