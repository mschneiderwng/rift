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
  types = lib.types;

  common = (import ../common.nix { inherit config pkgs lib; });
  inherit (common) allow unallow escapeUnitName;

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

  mkSnapshotTimer = id: cfg: schedule: timerConfig: {
    name = "rift-snapshot-${id}-${schedule}";
    value = {
      wantedBy = [ "timers.target" ];
      timerConfig = timerConfig;
    };
  };

  mkSnapshot =
    cfg: schedule: dataset:
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

  # create unit from someting like daily = ["rpool/user", "rpool/data"];
  mkSnapshotService =
    id: cfg: schedule: datasets:
    let
      unitName = "rift-snapshot-${id}-${schedule}";
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
          RestartMode = "direct";
          RestartSec = "60";
          ExecStartPre = allow user [ "snapshot" "bookmark" ] datasets;
          ExecStopPost = unallow user [ "snapshot" "bookmark" ] datasets;
          ExecStart = map (mkSnapshot cfg schedule) datasets;
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
        }
        // cfg.serviceConfig;
      };
    };
in
{
  options.services.rift.snapshots = lib.mkOption {
    type = types.attrsOf (
      types.submodule (
        { name, ... }:
        {
          options = {

            datasets = lib.mkOption {
              type = types.attrsOf (types.listOf (types.str));
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
              type = types.attrsOf (types.attrs);
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

            serviceConfig = lib.mkOption {
              type = types.attrs;
              default = { };
              description = "Systemd service configuration";
            };

            onFailure = lib.mkOption {
              type = types.listOf types.str;
              default = [ ];
              description = "Systemd OnFailure= dependencies.";
            };

            verbosity = lib.mkOption {
              type = types.str;
              description = ''Logging verbosity'';
              default = "-v";
            };
          };
        }
      )
    );

    description = ''
      rift snapshot services.
    '';
    example = ''
      services.rift.snapshots."system" = {
        datasets = "rpool/user" = [
          "frequently"
          "daily"
          "hourly"
          "weekly"
          "monthly"
          "yearly"
        ];
      };
    '';
    default = { };
  };

  config = lib.mkIf config.services.rift.enable {
    systemd.timers = lib.concatMapAttrs (
      id: cfg: lib.mapAttrs' (mkSnapshotTimer id cfg) cfg.schedules
    ) cfg;
    systemd.services = lib.concatMapAttrs (
      id: cfg: lib.mapAttrs' (mkSnapshotService id cfg) (invert cfg.datasets)
    ) cfg;
  };
}
