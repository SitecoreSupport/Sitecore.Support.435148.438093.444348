# Sitecore.Support.435148.438093.444348
  + 435148/435339: Data Provider selects EventQueue without the `NOLOCK` hint.
  + 438093: Data Provider converts fields in a very inefficient manner.
  + 444348: Data Provider check if a blob should be deleted in a very inefficient manner.

## Main

This repository contains Sitecore Patch #435148, #438093 and #444348, which fixes some performance issues with a Data Provider.

## Deployment

To apply the patch on both CM and CD servers perform the following steps:

1. Place the `Sitecore.Support.435148.438093.444348.config.dll` assembly into the `\bin` directory.
2. Place the `z.Sitecore.Support.435148.438093.444348.config.config` file into the `\App_Config\Include` directory.

## Content 

Sitecore Patch includes the following files:

1. `\bin\Sitecore.Support.435148.438093.444348.dll`
2. `\App_Config\Include\z.Sitecore.Support.435148.438093.444348.config`

## License

This patch is licensed under the [Sitecore Corporation A/S License](./LICENSE).

## Download

Downloads are available via [GitHub Releases](https://github.com/SitecoreSupport/Sitecore.Support.435148.438093.444348/releases).