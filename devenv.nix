{ pkgs, lib, config, inputs, ... }:

{
  packages = with pkgs; [ pandoc git ];

  languages = {
    python = {
      enable = true;

      poetry = {
        enable = true;

        install.enable  = true;
        activate.enable = true;
      };
    };
  };

  processes = {
    jupyter = {
      exec = "poetry run jupyter lab --ServerApp.token=''";
    };

    prefect = {
      exec = "poetry run prefect server start";
    };
  };
}
