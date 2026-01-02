self:
{
  config,
  pkgs,
  lib,
  ...
}:
let
  cfg = config.services.rift.prune;
  rift = "${self.packages.${pkgs.stdenv.hostPlatform.system}.rift}";
  types = lib.types;

  common = (import ../common.nix { inherit config pkgs lib; });
  inherit (common) allow unallow escapeUnitName;

  mkPolicy =
    cfg: policy:
    lib.lists.flatten (
      lib.mapAttrsToList (tag: keep: [
        "--keep"
        "rift_.*_${tag}"
        (toString keep)
      ]) policy
    );

  mkPrune =
    cfg: dataset: policy:
    lib.escapeShellArgs (
      [
        "${rift}/bin/rift"
        "prune"
      ]
      ++ lib.optional (cfg.verbosity != "") cfg.verbosity
      ++ (mkPolicy cfg policy)
      ++ [ dataset ]
    );

  mkPruneService =
    id: cfg:
    let
      user = cfg.name;
    in
    {
      name = cfg.name;
      value = {
        description = "rift prune service";
        onFailure = cfg.onFailure;
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
          ExecStartPre = allow user [ "destroy" "mount" ] (builtins.attrNames cfg.datasets);
          ExecStopPost = unallow user [ "destroy" "mount" ] (builtins.attrNames cfg.datasets);
          ExecStart = lib.mapAttrsToList (mkPrune cfg) cfg.datasets;
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

  mkPruneTimer = target: cfg: {
    name = cfg.name;
    value = {
      wantedBy = [ "timers.target" ];
      timerConfig = cfg.timerConfig;
    };
  };

in
{
  options.services.rift.prune = lib.mkOption {
    type = types.attrsOf (
      types.submodule (
        { name, ... }:
        {
          options = {
            datasets = lib.mkOption {
              type = types.attrsOf (types.attrsOf types.int);
              default = { };
              example = {
                "rpool/user" = {
                  frequently = 48 * 60 / 15;
                  hourly = 24;
                  daily = 30;
                  monthly = 12;
                  yearly = 0;
                };
              };
              description = ''
                Mapping of ZFS datasets to a retention policy.
                Each key is a dataset, and each value is an attrset of schedule → keep count (integer).
              '';
            };

            name = lib.mkOption {
              type = types.str;
              description = ''Systemd unit name.'';
              default = "rift-prune-${escapeUnitName name}";
            };

            timerConfig = lib.mkOption {
              type = types.attrs;
              default = {
                OnCalendar = "daily";
                RandomizedDelaySec = "10min";
                Persistent = true;
              };
              description = "Systemd timer configuration";
            };

            serviceConfig = lib.mkOption {
              type = types.attrs;
              default = { };
              description = "systemd service configuration";
            };

            onFailure = lib.mkOption {
              type = types.listOf types.str;
              default = [ ];
              description = "systemd OnFailure= dependencies.";
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
      rift prune services and timers.
    '';
    example = ''
      services.rift.prune."system" = {
        onFailure = [ "notify-email@%n.service" ];
        datasets = {
          "rpool/user" = {
            frequently = 48 * 60 / 15;
            hourly = 24;
            daily = 30;
            monthly = 12;
            yearly = 0;
          };
        };
      };
    '';
    default = { };
  };

  config = lib.mkIf config.services.rift.enable {
    systemd.timers = lib.mapAttrs' mkPruneTimer cfg;
    systemd.services = lib.mapAttrs' mkPruneService cfg;
  };
}
