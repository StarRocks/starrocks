// @ts-check
// `@type` JSDoc annotations allow editor autocompletion and type checking
// (when paired with `@ts-check`).
// There are various equivalent ways to declare your Docusaurus config.
// See: https://docusaurus.io/docs/api/docusaurus-config

import { themes as prismThemes } from "prism-react-renderer";

// if the env var DISABLE_VERSIONING is set
// (example `export DISABLE_VERSIONING=true`) then build only the
// content of `docs`. To build all versions remove the env var with
// `unset DISABLE_VERSIONING` (don't set it to false, we are checking
// to see if the var is set, not what the value is).
const isVersioningDisabled = !!process.env.DISABLE_VERSIONING || false;

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: "CelerData Enterprise",
  tagline: "CelerData Enterprise documentation",
  favicon: "img/favicon.ico",

  url: process.env.DOCUSAURUS_URL || "https://docs-sandbox.celerdata.com/",
  // Set the /<baseUrl>/ pathname under which your site is served
  baseUrl: "/Enterprise/",

  // needed for hosting in S3:
  trailingSlash: true,

  onBrokenAnchors: "ignore",
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "throw",

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
    localeConfigs: {
      en: {
        htmlLang: "en-US",
      },
    },
  },

  presets: [
    [
      "classic",
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve("./sidebars.json"),

          admonitions: {
            keywords: [
              "experimental",
              "beta",
              "note",
              "tip",
              "info",
              "caution",
              "danger",
            ],
          },
          // Versions:
          // We don't want to show `main` or `current`, we want to show the released versions.
          // lastVersion identifies the latest release.
          // onlyIncludeVersions limits what we show.
          // By default Docusaurus shows an "unsupported" banner, but we support multiple
          // versions, so the banner is set to none on the versions other than latest (latest
          // doesn't get a banner by default).
          lastVersion: (() => {
            if (isVersioningDisabled) {
              return "current";
            } else {
              return "3.2";
            }
          })(),

          onlyIncludeVersions: (() => {
            if (isVersioningDisabled) {
              return ["current"];
            } else {
              return ["3.2", "3.3"];
            }
          })(),

          versions: (() => {
            if (isVersioningDisabled) {
              return { current: { label: "current" } };
            } else {
              return {
                3.2: { label: "Stable-3.2", banner: "none" },
                3.3: { label: "Latest-3.3", banner: "none" },
              };
            }
          })(),
        },
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
        gtag: {
          trackingID: "G-VTBXVPZLHB",
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
      image: "img/CelerDataEnterprise.svg",
      navbar: {
        logo: {
          alt: "CelerData Enterprise logo",
          src: "img/CelerDataEnterprise.svg",
          href: "https://www.CelerData.com/",
        },
        items: [
          {
            label: "Privacy policy",
            position: "right",
            to: "https://celerdata.com/celerdata-privacy-policy",
          },
        ],
      },

      footer: {
        style: "dark",
        links: [
          {
            title: "Products",
            // Please don't remove the privacy and terms, it's a legal
            // requirement.
            items: [
              {
                label: "Product Overview",
                href: "https://celerdata.com/celerdata-products",
              },
              {
                label: "CelerData Cloud BYOC",
                href: "https://celerdata.com/celerdata-cloud",
              },
              {
                label: "Request a Demo",
                href: "https://celerdata.com/request-a-demo-celerdata-starrocks",
              },
            ],
          },
          {
            title: "Resources",
            // Please don't remove the privacy and terms, it's a legal
            // requirement.
            items: [
              {
                label: "CelerData Blog",
                href: "https://celerdata.com/blog",
              },
              {
                label: "White Papers",
                href: "https://celerdata.com/celerdata-white-papers-and-case-studies#white_papers",
              },
              {
                label: "Case Studies",
                href: "https://celerdata.com/celerdata-white-papers-and-case-studies#case_studies",
              },
              {
                label: "Glossary",
                href: "https://celerdata.com/glossary",
              },
              {
                label: "Events",
                href: "https://celerdata.com/events",
              },
            ],
          },
          {
            title: "StarRocks",
            // Please don't remove the privacy and terms, it's a legal
            // requirement.
            items: [
              {
                label: "StarRocks Homepage",
                href: "https://www.starrocks.io/",
              },
              {
                label: "Documentation",
                href: "https://docs.starrocks.io/docs/introduction/StarRocks_intro/",
              },
              {
                label: "GitHub",
                href: "https://github.com/StarRocks/StarRocks",
              },
            ],
          },
          {
            title: "Legal",
            // Please don't remove the privacy and terms, it's a legal
            // requirement.
            items: [
              {
                label: "Privacy Policy",
                href: "https://celerdata.com/celerdata-privacy-policy",
              },
              {
                label: "Terms of Use",
                href: "https://celerdata.com/celerdata-terms-of-use",
              },
              {
                label: "Cookie Policy",
                href: "https://celerdata.com/celerdata-cookie-policy",
              },
              {
                label: "Trademarks",
                href: "https://celerdata.com/celerdata-trademarks",
              },
            ],
          },
        ],
        logo: {
          alt: "All the Lakehouse Performance You Need to Ditch Your Data Warehouse",
          src: "/img/CelerDataWhite.png",
          href: "https://celerdata.com",
        },
        copyright: `Copyright © ${new Date().getFullYear()} CelerData, Inc.`,
      },

      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: [
          "java",
          "haskell",
          "python",
          "matlab",
          "bash",
          "diff",
          "json",
          "scss",
        ],
      },
      algolia: {
        // The application ID provided by Algolia
	    // the default is for SANDBOX
        appId: process.env.ALGOLIA_APPID || 'WR1QU4CYKE',
  
        // Public API key: it is safe to commit it
	    // the default is for SANDBOX
        apiKey: process.env.ALGOLIA_SEARCH_KEY || '83bf26ca88ddce9f0438d0e94db4e44a',
  
	    // the default is for SANDBOX
        indexName: process.env.ALGOLIA_INDEX || 'docs-sandbox-celerdata-enterprise',
  
        contextualSearch: true,
  
        // Optional: Algolia search parameters
        searchParameters: {},

        // Optional: path for search page that enabled by default (`false` to disable it)
        searchPagePath: 'search',
      },
    }),
};

module.exports = config;
