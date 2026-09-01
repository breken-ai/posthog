import type { Meta, StoryObj } from '@storybook/react'

import { DashboardPlacement, DashboardTile, InsightColor, QueryBasedInsightModel } from '~/types'

import { DashboardImageTile } from 'products/dashboards/frontend/components/ImageTile/DashboardImageTile'

const IMAGE_URL = `data:image/svg+xml,${encodeURIComponent(
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 600"><rect width="1200" height="600" fill="#c94c4c"/><circle cx="600" cy="300" r="180" fill="#f4c95d"/></svg>'
)}`

const transparentTile: DashboardTile<QueryBasedInsightModel> = {
    id: 1,
    color: InsightColor.White,
    transparent_background: true,
}

const image = {
    src: IMAGE_URL,
    alt: 'Abstract red and yellow image',
    title: '',
    layout: 'contain' as const,
    position: { x: 50, y: 50 },
}

const meta: Meta<typeof DashboardImageTile> = {
    title: 'Products/Dashboards/Tiles/Dashboard Image Tile',
    component: DashboardImageTile,
    parameters: {
        layout: 'fullscreen',
    },
    args: {
        tile: transparentTile,
        image,
        placement: DashboardPlacement.Dashboard,
        className: 'm-8 h-96 w-full max-w-3xl',
    },
}

export default meta
type Story = StoryObj<typeof DashboardImageTile>

export const Contain: Story = {}

export const Cover: Story = {
    args: {
        image: {
            ...image,
            layout: 'cover',
            position: { x: 75, y: 50 },
        },
    },
}

export const OpaqueCard: Story = {
    args: {
        tile: {
            ...transparentTile,
            transparent_background: false,
        },
    },
}
