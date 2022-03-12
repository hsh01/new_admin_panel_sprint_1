from django.contrib import admin
from .models import Genre, Filmwork, GenreFilmwork, Person, PersonFilmWork
from django.utils.translation import gettext_lazy as _


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)

    search_fields = ('title', 'description', 'id')


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork
    extra = 0

    verbose_name = _('genre')
    verbose_name_plural = _('genres')
    ordering = ('genre__name',)


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmWork
    extra = 0

    verbose_name = _('person')
    verbose_name_plural = _('persons')

    raw_id_fields = ('person',)

    ordering = ('role', 'person__full_name',)


class RatingListFilter(admin.SimpleListFilter):
    title = _('rating')

    parameter_name = 'rating'

    def lookups(self, request, model_admin):
        return ((str(k * 10), '{}-{}'.format(k * 10, k * 10 + 10)) for k in range(10))

    def queryset(self, request, queryset):
        if self.value():
            rate = int(self.value())
            return queryset.filter(rating__gte=rate, rating__lte=rate + 10)


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline,)

    list_display = ('title', 'type', 'creation_date', 'rating',)

    list_filter = ('type', RatingListFilter,)

    search_fields = ('title', 'description', 'id',)

    save_on_top = True

    def save_related(self, request, form, formsets, change):
        form.save_m2m()
        for formset in formsets:
            self.save_formset(request, form, formset, change=change)

    class Media:
        css = {
            'all': (
                'admin/css/inline_raw_id__wide.css',
            )
        }


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name',)
    search_fields = ('full_name',)
