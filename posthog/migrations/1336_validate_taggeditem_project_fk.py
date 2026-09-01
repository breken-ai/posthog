from django.db import migrations

from posthog.migration_helpers import ValidateForeignKey


class Migration(migrations.Migration):
    # VALIDATE scans under SHARE UPDATE EXCLUSIVE, which must not run inside the migration's
    # transaction alongside other work.
    atomic = False

    dependencies = [
        ("posthog", "1335_taggeditem_project_fk"),
    ]

    operations = [
        ValidateForeignKey(
            model_name="taggeditem",
            name="posthog_taggeditem_project_id_fk",
        ),
    ]
