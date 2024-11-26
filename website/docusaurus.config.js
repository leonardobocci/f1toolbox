// @ts-check
// `@type` JSDoc annotations allow editor autocompletion and type checking
// (when paired with `@ts-check`).
// There are various equivalent ways to declare your Docusaurus config.
// See: https://docusaurus.io/docs/api/docusaurus-config

import {themes as prismThemes} from 'prism-react-renderer';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'F1Toolbox',
  tagline: 'A data engineering end-to-end project showcase with Formula 1 data.',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://f1toolbox.com',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
        },
        blog: {
          showReadingTime: true,
          feedOptions: {
            type: ['rss', 'atom'],
            xslt: true,
          },
          // Useful options to enforce blogging best practices
          onInlineTags: 'warn',
          onInlineAuthors: 'warn',
          onUntruncatedBlogPosts: 'warn',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      image: 'img/docusaurus-social-card.jpg',
      navbar: {
        title: 'F1Toolbox',
        logo: {
          alt: 'F1Toolbox Logo',
          src: 'img/logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docsSidebar',
            position: 'left',
            label: 'Docs',
          },
          {to: '/blog', label: 'Build Story', position: 'left'},
          {
            href: 'https://dagster.f1toolbox.com',
            label: 'Dagster Playground',
            position: 'left',
          },
          {
            href: 'https://github.com/leonardobocci/f1toolbox',
            label: 'Code Repo',
            position: 'right',
          },
          {
            href: 'https://github.com/leonardobocci/f1toolbox-infra',
            label: 'Infrastucture Repo',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'F1Toolbox',
            items: [
              {
                label: 'Home',
                to: '/',
              },
              {
                label: 'Docs',
                to: '/docs/category/f1toolbox---overview',
              },
              {
                label: 'Build Story',
                to: '/blog',
              },
            ],
          },
          {
            title: 'External',
            items: [
              {
                label: 'Github',
                href: 'https://github.com/leonardobocci',
              },
              {
                label: 'Linkedin',
                href: 'https://www.linkedin.com/in/leonardobocci/',
              },
              {
                label: 'Portfolio Website',
                href: 'https://leobocci.pages.dev/',
              },
            ],
          },
          {
            title: 'Data Sources',
            items: [
              {
                label: 'Fastf1',
                href: 'https://github.com/theOehrly/Fast-F1/',
              },
              {
                label: 'Fantasy F1',
                href: 'https://f1fantasytools.com/',
              },
            ],
          },
        ],
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
      },
    }),
};

export default config;
