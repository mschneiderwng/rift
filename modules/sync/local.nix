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
  types = lib.types;

  common = (import ../common.nix { inherit config pkgs lib; });
  inherit (common) allow unallow escapeUnitName;

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
      user = cfg.name;
      target_ds = map (dataset: "${target}/${dataset}") cfg.datasets; # prepend target to datasets
      parents_ds = map (ds: builtins.dirOf ds) target_ds; # set allow permissions for parents of targets
    in
    {
      name = cfg.name;
      value = {
        description = "rift sync service";
        after = [ "zfs.target" ];
        startLimitBurst = 3;
        startLimitIntervalSec = 60 * 5;
        serviceConfig = {
          User = user;
          Group = user;
          StateDirectory = [ "rift/${cfg.name}" ];
          StateDirectoryMode = "700";
          CacheDirectory = [ "rift/${cfg.name}" ];
          CacheDirectoryMode = "700";
          RuntimeDirectory = [ "rift/${cfg.name}" ];
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
    name = cfg.name;
    value = {
      wantedBy = [ "timers.target" ];
      timerConfig = cfg.timerConfig;
    };
  };

in
{
  options.services.rift.sync.local = lib.mkOption {
    type = types.attrsOf (
      types.submodule (
        { name, ... }:
        {
          options = {
            datasets = lib.mkOption {
              type = types.listOf types.str;
              description = ''
                List of local ZFS datasets that should be replicated to this target.
              '';
              example = [
                "rpool/.../dev"
                "rpool/.../docs"
              ];
            };

            name = lib.mkOption {
              type = types.nullOr types.str;
              description = ''Systemd unit name.'';
              default = "rift-sync-${escapeUnitName name}";
            };

            filter = lib.mkOption {
              type = types.str;
              description = ''A regex matching the snapshots to be sent.'';
              default = "rift_.*_.*(?<!frequently)$"; # all but frequently
            };

            pipes = lib.mkOption {
              type = types.listOf types.str;
              default = [ ];
              example = [
                "pv -p -e -t -r -a -b -s {size}"
              ];
              description = "Programs to pipe to between send and recv.";
            };

            zfsSendOptions = lib.mkOption {
              type = types.listOf types.str;
              default = [ ];
              example = [ "-w" ];
              description = "Options passed to zfs send.";
            };

            zfsRecvOptions = lib.mkOption {
              type = types.listOf types.str;
              default = [ ];
              example = [
                "-s"
                "-u"
                "-F"
              ];
              description = "Options passed to zfs recv.";
            };

            verbosity = lib.mkOption {
              type = types.str;
              description = ''Logging verbosity'';
              default = "-v";
            };

            extraArgs = lib.mkOption {
              type = types.listOf types.str;
              default = [ ];
              description = "Extra rift arguments.";
            };

            timerConfig = lib.mkOption {
              type = types.attrs;
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
      services.rift.sync.local.targets."rift-recv@nas" = {
        datasets = [ "rpool/.../dev" "rpool/.../docs" ];
      };
    '';
    default = { };
  };

  config = lib.mkIf config.services.rift.enable {
    users.groups."rift" = { };
    users.users."rift" = {
      group = "rift";
      isSystemUser = true;
    };

    systemd.timers = lib.mapAttrs' mkSyncTimer cfg;
    systemd.services = lib.mapAttrs' mkSyncService cfg;
  };
}
