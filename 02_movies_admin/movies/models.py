import uuid

from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _


class CreatedAtMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True, verbose_name=_('created'))

    class Meta:
        abstract = True


class TimeStampedMixin(CreatedAtMixin):
    modified = models.DateTimeField(auto_now=True, verbose_name=_('modified'))

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('genre')
        verbose_name_plural = _('genres')

    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full_name'), max_length=255)

    def __str__(self):
        return self.full_name

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('person')
        verbose_name_plural = _('persons')


class Filmwork(UUIDMixin, TimeStampedMixin):
    class FilmworkType(models.TextChoices):
        MOVIE = 'movie', _('movie')
        TV_SHOW = 'tv_show', _('tv_show')

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    title = models.CharField(_('title'), max_length=255)
    description = models.TextField(_('description'), blank=True, null=True)
    creation_date = models.DateField(_('creation_date'))
    # file_path = models.FileField(_('file'), blank=True, null=True, upload_to='movies/')
    rating = models.FloatField(_('rating'), blank=True, validators=[MinValueValidator(0), MaxValueValidator(100)])
    type = models.CharField(_('type'), choices=FilmworkType.choices, max_length=7)
    persons = models.ManyToManyField(Person, through='PersonFilmWork', verbose_name=_('persons'))
    genres = models.ManyToManyField(Genre, through='GenreFilmwork', verbose_name=_('genre'))

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('filmwork')
        verbose_name_plural = _('filmworks')

    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin, CreatedAtMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE, verbose_name=_('filmwork'))
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE, verbose_name=_('genre'))

    class Meta:
        db_table = "content\".\"genre_film_work"

    def __str__(self):
        return str(self.genre.id)


class PersonFilmWork(UUIDMixin, CreatedAtMixin):

    class RoleType(models.TextChoices):
        ACTOR = 'actor', _('actor')
        WRITER = 'writer', _('writer')
        DIRECTOR = 'director', _('director')
        PRODUCER = 'producer', _('producer')

    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE, verbose_name=_('filmwork'))
    person = models.ForeignKey(Person, on_delete=models.CASCADE, verbose_name=_('person'))
    role = models.CharField(_('role'), choices=RoleType.choices, max_length=255, null=True)

    class Meta:
        db_table = "content\".\"person_film_work"
