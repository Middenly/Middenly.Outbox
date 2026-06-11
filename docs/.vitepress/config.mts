import { defineConfig, type DefaultTheme, type UserConfig } from 'vitepress'

const config: UserConfig<DefaultTheme.Config> = {
    base: '/',
    lang: 'en-US',
    title: 'Middenly',
    description: 'Production-ready .NET libraries for distributed systems',
    head: [
        ['link', {rel: 'icon', type: 'image/png', href: '/favicon.png'}],
        ['meta', {name: 'viewport', content: 'width=device-width, initial-scale=1.0'}],
        ['meta', {property: 'og:type', content: 'website'}],
        ['meta', {property: 'og:title', content: 'Middenly'}],
        ['meta', {property: 'og:description', content: 'Production-ready .NET libraries for distributed systems'}],
    ],
    lastUpdated: true,
    themeConfig: {
        logo: '/logo.png',

        nav: [
            {text: 'Home', link: '/'},
            {text: 'Outbox', link: '/outbox/guide/outbox-pattern'},
            {
                text: 'NuGet',
                link: 'https://www.nuget.org/packages/Middenly.Outbox'
            },
        ],

        search: {
            provider: 'local'
        },

        editLink: {
            pattern: 'https://github.com/Middenly/Middenly.Outbox/edit/main/docs/:path',
            text: 'Suggest changes to this page'
        },

        socialLinks: [
            {
                icon: 'github',
                link: 'https://github.com/Middenly/Middenly.Outbox'
            },
        ],

        footer: {
            message: 'Released under the MIT License.',
            copyright: 'Copyright © Middenly.',
        },

        sidebar: {
            '/outbox/': [
                {
                    text: 'Guide',
                    collapsed: false,
                    items: [
                        {text: 'The Outbox Pattern', link: '/outbox/guide/outbox-pattern'},
                        {text: 'Configuration', link: '/outbox/guide/configuration'},
                        {text: 'PostgreSQL Store', link: '/outbox/guide/postgresql'},
                        {text: 'Kafka Producer', link: '/outbox/guide/kafka'},
                        {text: 'Per-Topic Config', link: '/outbox/guide/topic-configuration'},
                        {text: 'Serialization', link: '/outbox/guide/serialization'},
                        {text: 'Dead Letter Queue', link: '/outbox/guide/dead-letter'},
                        {text: 'EF Core Integration', link: '/outbox/guide/efcore'},
                        {text: 'Database Schema', link: '/outbox/guide/schema'},
                    ]
                },
                {
                    text: 'Tutorials',
                    collapsed: false,
                    items: [
                        {text: 'Quickstart', link: '/outbox/tutorials/quickstart'},
                        {text: 'Integration Testing', link: '/outbox/tutorials/testing'},
                    ]
                },
            ]
        }
    },
    markdown: {
        linkify: false,
    },
    ignoreDeadLinks: true,
}

export default defineConfig(config)
