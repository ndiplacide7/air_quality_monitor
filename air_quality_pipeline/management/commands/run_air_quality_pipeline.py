from django.core.management.base import BaseCommand
from django.utils import timezone
from air_quality_pipeline.tasks import fetch_and_process_air_quality_data


class Command(BaseCommand):
    help = 'Manually trigger air quality data pipeline'

    def handle(self, *args, **options):
        self.stdout.write('Starting Air Quality Data Pipeline...')

        try:
            # Execute the task
            result = fetch_and_process_air_quality_data.delay()

            print(result)

            self.stdout.write(
                self.style.SUCCESS(
                    f'Pipeline task submitted. Task ID: {result.id}'
                )
            )

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Pipeline execution failed: {str(e)}')
            )