self:
{
  config,
  pkgs,
  lib,
  ...
}:
let
  cfg = config.services.rift.snapshots;
  rift = "${self.packages.${pkgs.stdenv.hostPlatform.system}.rift}";

  # Escape as required by: https://www.freedesktop.org/software/systemd/man/systemd.unit.html
  escapeUnitName =
    name:
    lib.concatMapStrings (s: if lib.isList s then "-" else s) (
      builtins.split "[^a-zA-Z0-9_.\\-]+" name
    );

  mkPermissions =
    action: user: permissions: dataset:
    lib.escapeShellArgs [
      "-+/run/booted-system/sw/bin/zfs"
      action
      user
      (lib.concatStringsSep "," permissions)
      dataset
    ];

  allow =
    user: perm: datasets:
    (map (mkPermissions "allow" user perm) datasets);

  unallow =
    user: permissions: datasets:
    (map (mkPermissions "unallow" user permissions) datasets);

  mkSnapshotTimer = schedule: timerConfig: {
    name = "rift-snapshot-${schedule}";
    value = {
      wantedBy = [ "timers.target" ];
      timerConfig = timerConfig;
    };
  };

  mkSnapshot =
    schedule: dataset:
    lib.escapeShellArgs (
      [
        "${rift}/bin/rift"
        "snapshot"
      ]
      ++ lib.optional (cfg.verbosity != "") cfg.verbosity
      ++ [
        "--name"
        "rift_{datetime}_${schedule}"
      ]
      ++ [ dataset ]
    );

  mkSnapshotService =
    schedule: datasets:
    let
      unitName = "rift-snapshot-${schedule}";
      user = unitName;
    in
    {
      name = unitName;
      value = {
        description = "rift snapshot service";
        onFailure = cfg.onFailure;
        after = [ "zfs.target" ];
        startLimitBurst = 3;
        startLimitIntervalSec = 60 * 5;
        serviceConfig = {
          User = user;
          Group = user;
          StateDirectory = [ "rift/${unitName}" ];
          StateDirectoryMode = "700";
          CacheDirectory = [ "rift/${unitName}" ];
          CacheDirectoryMode = "700";
          RuntimeDirectory = [ "rift/${unitName}" ];
          RuntimeDirectoryMode = "700";
          Type = "oneshot";
          Restart = "on-failure";
          RestartSec = "60";
          ExecStartPre = allow user [ "snapshot" "bookmark" ] datasets;
          ExecStopPost = unallow user [ "snapshot" "bookmark" ] datasets;
          ExecStart = map (mkSnapshot schedule) datasets;
          CPUWeight = 20;
          CPUQuota = "75%";
          BindPaths = [ "/dev/zfs" ];
          DeviceAllow = [ "/dev/zfs" ];
          CapabilityBoundingSet = "";
          DevicePolicy = "closed";
          DynamicUser = true;
          LockPersonality = true;
          MemoryDenyWriteExecute = true;
          NoNewPrivileges = true;
          PrivateDevices = true;
          PrivateMounts = true;
          PrivateNetwork = true;
          PrivateTmp = true;
          PrivateUsers = false;
          ProtectClock = true;
          ProtectControlGroups = true;
          ProtectHome = true;
          ProtectHostname = true;
          ProtectKernelLogs = true;
          ProtectKernelModules = true;
          ProtectKernelTunables = true;
          ProtectProc = "invisible";
          ProtectSystem = "strict";
          RestrictAddressFamilies = [ "none" ];
          RestrictNamespaces = true;
          RestrictRealtime = true;
          RestrictSUIDSGID = true;
          SystemCallArchitectures = "native";
          SystemCallFilter = [
            " " # This is needed to clear the SystemCallFilter existing definitions
            "~@reboot"
            "~@swap"
            "~@obsolete"
            "~@mount"
            "~@module"
            "~@debug"
            "~@cpu-emulation"
            "~@clock"
            "~@raw-io"
            "~@privileged"
            "~@resources"
          ];
          UMask = 0077;
        };
      };
    };

  # Turn: { "rpool/user" = [ "daily" "weekly" ]; } into { daily = ["rpool/user"]; weekly = ["rpool/user"]; }
  invert =
    ds:
    lib.mapAttrs (_: es: map (e: e.dataset) es) (
      lib.groupBy (e: e.schedule) (
        lib.concatLists (
          lib.mapAttrsToList (dataset: schedules: map (schedule: { inherit schedule dataset; }) schedules) ds
        )
      )
    );

in
{
  options.services.rift.snapshots = {
    enable = lib.mkEnableOption "rift snapshot service";

    datasets = lib.mkOption {
      type = lib.types.attrsOf (lib.types.listOf (lib.types.str));
      default = { };
      example = {
        "rpool/user" = [
          "frequently"
          "daily"
          "hourly"
          "weekly"
          "monthly"
          "yearly"
        ];
      };
      description = ''Mapping of ZFS datasets to a list of snapshot schedules.'';
    };

    schedules = lib.mkOption {
      type = lib.types.attrsOf (lib.types.attrs);
      default = {
        frequently = {
          OnCalendar = "*:0/15";
          Persistent = false;
          RandomizedDelaySec = "1m";
        };
        hourly = {
          OnCalendar = "hourly";
          Persistent = false;
          RandomizedDelaySec = "1m";
        };
        daily = {
          OnCalendar = "daily";
          Persistent = true;
          RandomizedDelaySec = "1m";
        };
        weekly = {
          OnCalendar = "weekly";
          Persistent = true;
          RandomizedDelaySec = "1m";
        };
        monthly = {
          OnCalendar = "monthly";
          Persistent = true;
          RandomizedDelaySec = "1m";
        };
        yearly = {
          OnCalendar = "yearly";
          Persistent = true;
          RandomizedDelaySec = "1m";
        };
      };
      description = ''Mapping scheduls/tags to systemd timer configs.'';
    };

    onFailure = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "systemd OnFailure= dependencies.";
    };

    verbosity = lib.mkOption {
      type = lib.types.str;
      description = ''Logging verbosity'';
      default = "-v";
    };
  };

  config = lib.mkIf cfg.enable {

    environment.systemPackages = with pkgs; [ rift ];

    systemd.timers = lib.mapAttrs' mkSnapshotTimer cfg.schedules;
    systemd.services = lib.mapAttrs' mkSnapshotService (invert cfg.datasets);
  };
}
