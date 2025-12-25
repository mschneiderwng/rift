self:
{
  config,
  pkgs,
  lib,
  ...
}:
let
  cfg = config.services.rift.sync.local;
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

  mkSync =
    cfg: target: dataset:
    lib.escapeShellArgs (
      [
        "${rift}/bin/rift"
        "sync"
      ]
      ++ lib.optionals (cfg.filter != "") [
        "--filter"
        cfg.filter
      ]
      ++ lib.optional (cfg.verbosity != "") cfg.verbosity
      ++ cfg.extraArgs
      ++ builtins.concatMap (option: [
        "-S"
        option
      ]) cfg.zfsSendOptions
      ++ builtins.concatMap (option: [
        "-R"
        option
      ]) cfg.zfsRecvOptions
      ++ builtins.concatMap (pipe: [
        "-p"
        pipe
      ]) cfg.pipes
      ++ [
        "${dataset}"
        "${target}/${dataset}"
      ]
    );

  mkSyncService =
    target: cfg:
    let
      unitName = "rift-sync-${cfg.name}";
      user = unitName;
      target_ds = map (dataset: "${target}/${dataset}") cfg.datasets; # prepend target to datasets
      parents_ds = map (ds: builtins.dirOf ds) target_ds; # set allow permissions for parents of targets
    in
    {
      name = unitName;
      value = {
        description = "rift sync service";
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
          RestartMode = "direct";
          RestartSec = "60";
          ExecStartPre =
            (allow user [ "send" ] cfg.datasets)
            ++ (allow user [ "create" "receive" "mount" ] target_ds)
            ++ (allow user [ "create" "receive" "mount" ] parents_ds);
          ExecStopPost =
            (unallow user [ "send" ] cfg.datasets)
            ++ (unallow user [ "create" "receive" "mount" ] target_ds)
            ++ (unallow user [ "create" "receive" "mount" ] parents_ds);
          ExecStart = map (mkSync cfg target) cfg.datasets;
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
          PrivateNetwork = false;
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
          RestrictAddressFamilies = [
            "AF_UNIX"
            "AF_INET"
            "AF_INET6"
          ];
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

  mkSyncTimer = target: cfg: {
    name = "rift-sync-${cfg.name}";
    value = {
      wantedBy = [ "timers.target" ];
      timerConfig = cfg.timerConfig;
    };
  };

in
{
  options.services.rift.sync.local = {

    enable = lib.mkEnableOption "Enable rift ZFS sync service";

    targets = lib.mkOption {
      type = lib.types.attrsOf (
        lib.types.submodule (
          { target, config, ... }:
          {
            options = {

              datasets = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                description = ''
                  List of local ZFS datasets that should be replicated to this target.
                '';
                example = [
                  "rpool/.../dev"
                  "rpool/.../docs"
                ];
              };

              name = lib.mkOption {
                type = lib.types.nullOr lib.types.str;
                description = ''Systemd unit name.'';
                default = lib.mkDefault (escapeUnitName target);
              };

              filter = lib.mkOption {
                type = lib.types.str;
                description = ''A regex matching the snapshots to be sent.'';
                default = "rift_.*_.*(?<!frequently)$"; # all but frequently
              };

              pipes = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
                example = [
                  "pv -p -e -t -r -a -b -s {size}"
                ];
                description = "Programs to pipe to between send and recv.";
              };

              zfsSendOptions = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
                example = [ "-w" ];
                description = "Options passed to zfs send.";
              };

              zfsRecvOptions = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
                example = [
                  "-s"
                  "-u"
                  "-F"
                ];
                description = "Options passed to zfs recv.";
              };

              verbosity = lib.mkOption {
                type = lib.types.str;
                description = ''Logging verbosity'';
                default = "-v";
              };

              extraArgs = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
                description = "Extra rift arguments.";
              };

              timerConfig = lib.mkOption {
                type = lib.types.attrs;
                default = {
                  OnCalendar = "hourly";
                  RandomizedDelaySec = "10min";
                  Persistent = true;
                };
                description = "Systemd timer configuration.";
              };
            };
          }
        )
      );

      description = ''
        Mapping of target rift receivers to their sync configuration.
      '';
      example = ''
        services.rift.sync.targets."rift-recv@nas" = {
          datasets = [ "rpool/.../dev" "rpool/.../docs" ];
        };
      '';
    };

  };

  config = lib.mkIf cfg.enable {
    environment.systemPackages = with pkgs; [
      rift
    ];

    users.groups."rift" = { };
    users.users."rift" = {
      group = "rift";
      isSystemUser = true;
    };

    systemd.timers = lib.mapAttrs' mkSyncTimer cfg.targets;
    systemd.services = lib.mapAttrs' mkSyncService cfg.targets;
  };
}
