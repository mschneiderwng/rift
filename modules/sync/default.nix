self:
{
  config,
  pkgs,
  lib,
  ...
}:
let
  cfg = config.ash.services.rift.sync;
  rift = "${self.packages.${pkgs.system}.rift}";

  # Escape as required by: https://www.freedesktop.org/software/systemd/man/systemd.unit.html
  escapeUnitName =
    name:
    lib.concatMapStrings (s: if lib.isList s then "-" else s) (
      builtins.split "[^a-zA-Z0-9_.\\-]+" name
    );

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

  mkSync =
    cfg: remote: datasets:
    map (ds: "${rift}/bin/rift sync -vv ${cfg.sshOptions} ${ds} ${remote}/${ds}") datasets;

  mkSyncService = remote: cfg: {
    name = "rift-sync-${if cfg.name == null then (escapeUnitName remote) else cfg.name}";
    value = {
      description = "rift sync service";
      after = [ "zfs.target" ];
      path = [ pkgs.openssh ];
      serviceConfig = {
        LoadCredential = [ "ssh_key:${config.sops.secrets."rift/sync/key".path}" ];
        User = "rift";
        Group = "rift";
        CacheDirectory = [ "rift" ];
        CacheDirectoryMode = "700";
        Type = "oneshot";
        ExecStartPre = allow [ "send" ] cfg.datasets;
        ExecStopPost = unallow [ "send" ] cfg.datasets;
        ExecStart = mkSync cfg remote cfg.datasets;
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

  mkSyncTimer = remote: cfg: {
    name = "rift-sync-${if cfg.name == null then (escapeUnitName remote) else cfg.name}";
    value = {
      wantedBy = [ "timers.target" ];
      timerConfig = cfg.timerConfig;
    };
  };

in
{
  options.ash.services.rift.sync = {

    enable = lib.mkEnableOption "Enable rift ZFS sync service";

    remotes = lib.mkOption {
      type = lib.types.attrsOf (
        lib.types.submodule ({
          options = {

            datasets = lib.mkOption {
              type = lib.types.listOf lib.types.str;
              description = ''
                List of local ZFS datasets that should be replicated to this remote.
              '';
              example = [
                "rpool/.../dev"
                "rpool/.../docs"
              ];
            };

            name = lib.mkOption {
              type = lib.types.nullOr lib.types.str;
              description = ''Systemd unit name.'';
              default = null;
            };

            sshOptions = lib.mkOption {
              type = lib.types.str;
              description = ''Options passed to ssh.'';
              default = "-t ControlPath=/var/cache/rift/ssh-master -t ControlMaster=auto -t ControlPersist=60 -t IdentityFile=\${CREDENTIALS_DIRECTORY}/ssh_key";
            };

            timerConfig = lib.mkOption {
              type = lib.types.attrs;
              default = {
                OnCalendar = "hourly";
                RandomizedDelaySec = "10min";
                Persistent = true;
              };
              description = "systemd timer configuration";
            };
          };
        })
      );

      description = ''
        Mapping of remote rift receivers to their sync configuration.
      '';
      example = ''
        ash.services.rift.sync.remotes."rift-recv@nas" = {
          datasets = [ "rpool/.../dev" "rpool/.../docs" ];
        };
      '';
    };

  };

  config = lib.mkIf cfg.enable {
    ash.services.notify-email.enable = true;
    ash.programs.sops.enable = true;

    environment.systemPackages = with pkgs; [
      rift
      mbuffer
    ];

    sops.secrets."rift/sync/key" = { };

    users.groups."rift" = { };
    users.users."rift" = {
      group = "rift";
      isSystemUser = true;
    };

    systemd.timers = lib.mapAttrs' mkSyncTimer cfg.remotes;
    systemd.services = lib.mapAttrs' mkSyncService cfg.remotes;
  };
}
