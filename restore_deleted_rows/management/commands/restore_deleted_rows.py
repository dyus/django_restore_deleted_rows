# coding: utf-8
from __future__ import unicode_literals

from django.core.management.base import BaseCommand

class Command(BaseCommand):
    """ Команда для восстановления данных из
        бэкапа поднятого отдельной базой с названием 'restored'

        Логика команды очень простая:
            Идем по всем зарегистрированным в приложениях моделям, получаем объекты, которые есть в restored, но нет в
            default
            Найденные объекты пытаемся сохранить в бд, если возникает ошибка (обычно это будет проблема с ForeignKey),
            то записываем данные в словарь с ошибками для последующей специальной обработки

            exceptions_data = {
                'model': (exception, obj)
            }
            restored_data = {
                'model': obj
            }
    """
    def handle(self, *args, **options):

        from django.apps import apps
        from corportal.home.models import User
        from corportal.rds.models import Project

        def restore_data():

            # TODO параметризовать названия бд
            # TODO учесть модели, которые создаются автоматически и для корректной истории их нужно дропнуть и накатить
            # TODO предусмотреть создание many_to_many
            restored_db_alias = 'restore'
            default_db_alias = 'default'
            objects_for_recreation = {User: 'username'}
            projects_models = apps.get_models()

            exceptions_data = {}
            restored_data = {}
            restored_many_to_many = {}
            # восстанавливаем только записи, у которых pk.name = u'id'. Кажется что других и не надо
            # projects_models = [pm for pm in projects_models if pm._meta.pk.name == u'id']
            for project_model in projects_models:
                pk = project_model._meta.pk.name
                default_db_objects_pks = set(project_model.objects.using(default_db_alias).values_list(pk, flat=True))
                restored_db_objects_pks = set(project_model.objects.using(restored_db_alias).values_list(pk, flat=True))

                lost_objects_pks = restored_db_objects_pks - default_db_objects_pks
                if lost_objects_pks:
                    # если такие есть, то восстановим по ним данные
                    search_query = {'%s__in' % pk: lost_objects_pks}
                    lost_objects = project_model.objects.using(restored_db_alias).filter(**search_query)
                    for obj in lost_objects:
                        try:
                            # if project_model in objects_for_recreation.keys():
                            #     remove_key = objects_for_recreation[project_model]
                            #     remove_query = {remove_key: getattr(obj, remove_key)}
                            #     project_model.objects.using(default_db_alias).filter(**remove_query).delete()
                            obj.save(using=default_db_alias)
                            restored_data.setdefault(project_model, []).append(obj)

                            # many_to_many relation
                            many_to_many_relations = project_model._meta.many_to_many
                            restored_many_to_many = restore_groups(many_to_many_relations, project_model, restored_many_to_many)
                            # for many_field in many_to_many_relations:
                            #     default_obj = project_model.objects.using(default_db_alias).get(id=obj.id)
                            #     restore_obj = project_model.objects.using(restored_db_alias).get(id=obj.id)
                            #     default_many_related_manager = getattr(default_obj, many_field.name)
                            #     restored_many_related_manager = getattr(restore_obj, many_field.name)
                            #     restored_db_objects = list(restored_many_related_manager.all())
                            #     default_many_related_manager.clear()
                            #     default_db_objects_for_restore_relation = many_field.related_model.objects.using(default_db_alias)\
                            #         .filter(id__in=[o.id for o in restored_db_objects])
                            #
                            #     default_many_related_manager.add(*default_db_objects_for_restore_relation)
                            #     restored_many_to_many.setdefault(project_model, []).append(many_field)

                        except Exception as ex:
                            exceptions_data.setdefault(project_model, []).append((
                                ex, obj
                            ))
            return exceptions_data, restored_data, restored_many_to_many


        def restore_groups(many_to_many_relations, project_model, restored_many_to_many):
            restored_db_alias = 'restore'
            default_db_alias = 'default'
            for many_field in many_to_many_relations:
                query = {'groups__isnull': True}
                for obj in project_model.objects.filter(**query):
                    default_obj = project_model.objects.using(default_db_alias).get(id=obj.id)
                    restore_obj = project_model.objects.using(restored_db_alias).get(id=obj.id)
                    default_many_related_manager = getattr(default_obj, many_field.name)
                    restored_many_related_manager = getattr(restore_obj, many_field.name)
                    restored_db_objects = list(restored_many_related_manager.all())
                    default_many_related_manager.clear()
                    default_db_objects_for_restore_relation = many_field.related_model.objects.using(default_db_alias)\
                        .filter(id__in=[o.id for o in restored_db_objects])

                    default_many_related_manager.add(*default_db_objects_for_restore_relation)
                    restored_many_to_many.setdefault(project_model, []).append(many_field)
            return restored_many_to_many

        exceptions_data, restored_data, restored_many_to_many = restore_data()
