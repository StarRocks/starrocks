// @ts-check
// `@type` JSDoc annotations allow editor autocompletion and type checking
// (when paired with `@ts-check`).
// There are various equivalent ways to declare your Docusaurus config.
// See: https://docusaurus.io/docs/api/docusaurus-config

import {themes as prismThemes} from 'prism-react-renderer';

// if the env var DISABLE_VERSIONING is set
// (example `export DISABLE_VERSIONING=true`) then build only the
// content of `docs`. To build all versions remove the env var with
// `unset DISABLE_VERSIONING` (don't set it to false, we are checking
// to see if the var is set, not what the value is).
const isVersioningDisabled = !!process.env.DISABLE_VERSIONING || false;

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'CelerData',
  tagline: 'CelerData documentation',
  favicon: 'img/favicon.ico',

  url: 'https://docs.CelerData.com/',
  // Set the /<baseUrl>/ pathname under which your site is served
  baseUrl: '/',

  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'CelerData', // Usually your GitHub org/user name.
  projectName: 'CelerData', // Usually your repo name.

  // needed for hosting in S3:
  trailingSlash: true,

  onBrokenAnchors: 'ignore',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
    localeConfigs: {
      en: {
        htmlLang: 'en-US',
      },
    },
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.json'),

          // Versions:
          // We don't want to show `main` or `current`, we want to show the released versions.
          // lastVersion identifies the latest release.
          // onlyIncludeVersions limits what we show.
          // By default Docusaurus shows an "unsupported" banner, but we support multiple
          // versions, so the banner is set to none on the versions other than latest (latest
          // doesn't get a banner by default).
          lastVersion: (() => {
              if (isVersioningDisabled) {
                return 'current';
              } else {
                return '3.1';
              }
            })(),

          onlyIncludeVersions: (() => {
              if (isVersioningDisabled) {
                return ['current'];
              } else {
                return ['3.1', '3.0', '2.5', '2.3', '2.2', '2.1', '2.0', '1.19'];
              }
            })(),

          versions: (() => {
              if (isVersioningDisabled) {
                return { current: { label: 'current' } };
              } else {
                return {
                  '3.1': { label: 'Stable-3.1' },
                  '3.0': { label: '3.0', banner: 'none' },
                  '2.5': { label: '2.5', banner: 'none' },
                  '2.3': { label: '2.3', banner: 'none' },
                  '2.2': { label: '2.2', banner: 'none' },
                  '2.1': { label: '2.1', banner: 'none' },
                  '2.0': { label: '2.0', banner: 'none' },
                  '1.19': { label: '1.19', banner: 'none' },
                };
              }
            })(),

        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        gtag: {
          trackingID: 'G-VTBXVPZLHB',
          anonymizeIP: true,
        },
      }),
    ],
  ],
  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      docs: {
        sidebar: {
          hideable: true,
          autoCollapseCategories: true,
        },
      },
      // This image shows in Slack when you paste a link
      image: 'img/logo.svg',
      navbar: {
        title: 'CelerData',
        logo: {
          alt: 'CelerData Logo',
          src: 'img/logo.svg',
          href: 'https://www.CelerData.com/',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docs',
            position: 'left',
            label: 'Documentation',
          },
          {
            type: 'docsVersionDropdown',
            position: 'left',
            dropdownActiveClassDisabled: true,
          },
          {
            href: 'https://github.com/CelerData/',
            label: 'GitHub',
            position: 'right',
          },
          {
            type: 'localeDropdown',
            position: 'left',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Documentation',
                to: '/docs/introduction/',
              },
            ],
          },
        ],
        copyright: `Docs built with Docusaurus.`,
      },

      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: [
          'java',
          'haskell',
          'python',
          'matlab',
          'bash',
          'diff',
          'json',
          'scss',
        ],
      },
    }),
};

module.exports = config;
