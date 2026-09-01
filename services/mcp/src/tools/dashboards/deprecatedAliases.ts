import type { z } from 'zod'

import { DashboardTileCreateSchema } from '@/schema/tool-inputs'
import { GENERATED_TOOLS } from '@/tools/generated/dashboards'
import type { Context, ToolBase, ZodObjectAny } from '@/tools/types'

export const DASHBOARD_DEPRECATED_ALIASES: Record<string, () => ToolBase<ZodObjectAny>> = {
    'dashboard-create-text-tile': () => {
        const inner = GENERATED_TOOLS['dashboard-create-tile']!()
        return {
            ...inner,
            name: 'dashboard-create-text-tile',
            schema: DashboardTileCreateSchema.omit({ type: true }),
            handler: async (context: Context, params: z.infer<ZodObjectAny>) => ({
                ...((await inner.handler(context, { ...params, type: 'text' })) as object),
                _deprecation_notice:
                    'dashboard-create-text-tile has been renamed to dashboard-create-tile. Call dashboard-create-tile directly next time.',
            }),
        }
    },
}
