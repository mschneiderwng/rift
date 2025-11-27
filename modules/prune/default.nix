self:
{
  config,
  pkgs,
  lib,
  ...
}:
let
  cfg = config.ash.services.rift.prune;
  rift = "${self.packages.${pkgs.system}.rift}";

  join = builtins.concatStringsSep;

  mkPermissions =
    action: permissions: dataset:
    lib.escapeShellArgs [
      "-+/run/booted-system/sw/bin/zfs"
      action
      "rift"
      (lib.concatStringsSep "," permissions)
      dataset
    ];

  allow = perm: datasets: (map (mkPermissions "allow" perm) datasets);
  unallow = permissions: datasets: (map (mkPermissions "unallow" permissions) datasets);

  mkPolicy =
    policy: join " " (lib.mapAttrsToList (tag: keep: "--keep ${toString keep} rift_.*_${tag}") policy);

  mkPrune = dataset: policy: "${rift}/bin/rift prune -v ${mkPolicy policy} ${dataset}";

  attrKeys = attrs: lib.mapAttrsToList (name: value: name) attrs;
in
{
  options.ash.services.rift.prune = {
    enable = lib.mkEnableOption "rift prune service";

    datasets = lib.mkOption {
      type = lib.types.attrsOf (lib.types.attrsOf lib.types.int);
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

    timerConfig = lib.mkOption {
      type = lib.types.attrs;
      default = {
        OnCalendar = "daily";
        RandomizedDelaySec = "10min";
        Persistent = true;
      };
      description = "systemd timer configuration";
    };
  };

  config = lib.mkIf cfg.enable {
    ash.services.notify-email.enable = true;

    environment.systemPackages = with pkgs; [ rift ];

    systemd.timers."rift-prune" = {
      wantedBy = [ "timers.target" ];
      timerConfig = cfg.timerConfig;
    };

    systemd.services."rift-prune" = {
      description = "rift prune service";
      onFailure = [ "notify-email@%n.service" ];
      after = [ "zfs.target" ];
      serviceConfig = {
        User = "rift";
        Group = "rift";
        Type = "oneshot";
        RuntimeDirectory = "rift";
        CacheDirectory = "rift";
        ExecStartPre = allow [ "destroy" "mount" ] (attrKeys cfg.datasets);
        ExecStopPost = unallow [ "destroy" "mount" ] (attrKeys cfg.datasets);
        ExecStart = lib.mapAttrsToList mkPrune cfg.datasets;
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
}
