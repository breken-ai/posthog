import { useActions, useValues } from 'kea'

import { ObjectTags } from 'lib/components/ObjectTags/ObjectTags'
import { LemonSkeleton } from 'lib/lemon-ui/LemonSkeleton'
import { getAccessControlDisabledReason } from 'lib/utils/accessControlUtils'
import { projectLogic } from 'scenes/projectLogic'

import { tagsModel } from '~/models/tagsModel'
import { AccessControlLevel, AccessControlResourceType } from '~/types'

export function ProjectTags(): JSX.Element {
    const { currentProject, currentProjectLoading } = useValues(projectLogic)
    const { updateCurrentProject } = useActions(projectLogic)
    const { tags: tagsAvailable } = useValues(tagsModel)

    // Writing tags is a project write, so mirror the editor access the API itself requires.
    const editDisabledReason = getAccessControlDisabledReason(
        AccessControlResourceType.Project,
        AccessControlLevel.Editor
    )

    if (!currentProject) {
        return <LemonSkeleton className="w-40 h-5" />
    }

    if (editDisabledReason) {
        return <ObjectTags tags={currentProject.tags ?? []} staticOnly data-attr="project-tags" />
    }

    return (
        <ObjectTags
            tags={currentProject.tags ?? []}
            tagsAvailable={tagsAvailable}
            onChange={(tags) => updateCurrentProject({ tags })}
            saving={currentProjectLoading}
            data-attr="project-tags"
        />
    )
}
