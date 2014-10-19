
from collections import namedtuple
import os

import MySQLdb as mdb


_CACHED_PROPERTIES_KEY = '__cached_properties'


def cached_property(method):
    """
    Modification of @property to auto-cache the result on the object.

    To delete the cache, just del the attribute on the object. Code
    can also manually load the cached result -- useful for bulk loading.

    >>> class Foo(object):
    ...     @cached_property
    ...     def attr(self):
    ...         print 'heavy calculations'
    ...         return None

    >>> foo = Foo()
    >>> foo.attr
    heavy calculations
    >>> foo.attr
    >>> del foo.attr
    >>> foo.attr
    heavy calculations
    >>> del foo.attr
    >>> foo.attr = None
    >>> foo.attr
    """
    key = method.__name__

    def getter(self):
        cached_properties = self.__dict__.setdefault(
            _CACHED_PROPERTIES_KEY, {})
        try:
            return cached_properties[key]
        except KeyError:
            result = method(self)
            cached_properties[key] = result
            return result

    def setter(self, val):
        cached_properties = self.__dict__.setdefault(
            _CACHED_PROPERTIES_KEY, {})
        cached_properties[key] = val

    def deleter(self):
        cached_properties = self.__dict__.setdefault(
            _CACHED_PROPERTIES_KEY, {})
        try:
            del cached_properties[key]
        except KeyError:
            pass

    return property(getter, setter, deleter, method.__doc__)


def clear_cached_properties(object):
    """
    Clear all cached properties from an object.

    Returns the object.
    """
    try:
        del object.__dict__[_CACHED_PROPERTIES_KEY]
    except KeyError:
        pass
    return object


class FamiLinx(object):

    MIN_PERSON_ID = 1
    MAX_PERSON_ID = 43589549

    def __init__(self):
        self.conn = None
        self.cur = None
        self._people = {}

    def __iter__(self):
        return self.iter_people()

    def connect(self, host='localhost', user='familinx', passwd='',
                db='familinx'):
        self.conn = mdb.connect(host=host, user=user, passwd=passwd, db=db)
        self.cur = self.conn.cursor()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_person(self, id):
        person = self._people.get(id)
        if not person:
            self._people[id] = person = Person(id, self)
        return person

    def iter_people(self, start=MIN_PERSON_ID, end=MAX_PERSON_ID+1):
        for id in range(start, end):
            yield self.get_person(id)

    def clear_cache(self, id=None):
        if id:
            if id in self._people:
                del self._people[id]
        else:
            self._people.clear()


Location = namedtuple(
    'Location',
    ['lon', 'lat', 'country', 'continent', 'resolution'])

'''
Continents:
        AF : Africa
        AS : Asia
        EU : Europe
        NA : North America
        OC : Oceania
        SA : South America
        AN : Antarctica
'''

Founder = namedtuple(
    'Founder',
    ['nleaves', 'min_depth', 'max_depth', 'median_depth'])


class Person(object):

    GENDERS = {
        1: 'male',
        2: 'female',
    }

    def __init__(self, id=None, db=None):
        self.id = id
        self.db = db
        self._years = None

    def __hash__(self):
        return hash((Person, id))

    def __eq__(self, other):
        return self.id == other.id

    def __unicode__(self):
        return 'Person(id=%d)' % self.id

    def __repr__(self):
        return 'Person(id=%d)' % self.id

    @cached_property
    def age(self):
        sql = 'select Age from age where Id = %s'
        if self.db.cur.execute(sql, (self.id,)):
            return self.db.cur.fetchone()[0]

    @cached_property
    def years(self):
        sql = 'select Byear, Dyear from years where Id = %s'
        if self.db.cur.execute(sql, (self.id,)):
            return map(int, self.db.cur.fetchone())
        else:
            return (None, None)

    @property
    def birth_year(self):
        return self.years[0]

    @property
    def death_year(self):
        return self.years[1]

    @cached_property
    def parents(self):
        sql = 'select Parent_Id from relationship where Child_Id = %s'
        if self.db.cur.execute(sql, (self.id,)):
            return [self.db.get_person(row[0])
                    for row in self.db.cur]
        else:
            return []

    @property
    def mother(self):
        for parent in self.parents:
            if parent.gender == 'female':
                return parent

    @property
    def father(self):
        for parent in self.parents:
            if parent.gender == 'male':
                return parent

    @cached_property
    def children(self):
        sql = 'select Child_Id from relationship where Parent_Id = %s'
        if self.db.cur.execute(sql, (self.id,)):
            return [self.db.get_person(row[0])
                    for row in self.db.cur]
        else:
            return []

    @cached_property
    def partners(self):
        return list(set(parent for child in self.children
                        for parent in child.parents
                        if parent != self))

    @property
    def is_founder(self):
        return not self.parents

    @property
    def is_leaf(self):
        return not self.children

    @cached_property
    def gender(self):
        sql = 'select Gender from gender where Id = %s'
        if self.db.cur.execute(sql, (self.id,)):
            return self.GENDERS[self.db.cur.fetchone()[0]]

    @cached_property
    def location(self):
        sql = ('select Lon, Lat, Country, Continent, Res '
               'from location where Id = %s')
        if self.db.cur.execute(sql, (self.id,)):
            return Location(*self.db.cur.fetchone())

    @cached_property
    def founder_stats(self):
        sql = ('select Nleaves, MinG, MaxG, MedianG '
               'from founders where Founder = %s')
        if self.db.cur.execute(sql, (self.id,)):
            nleaves, min_depth, max_depth, median_depth = (
                self.db.cur.fetchone())
            return Founder(
                int(nleaves), int(min_depth), int(max_depth), median_depth)

    def stats(self):
        """
        Return a dict of common fields.
        """
        location = self.location
        birth, death = self.years
        return {
            'parents': [p.id for p in self.parents],
            'children': [c.id for c in self.children],
            'age': self.age,
            'birth': birth,
            'death': death,
            'gender': self.gender,

            'lon': location.lon if location else None,
            'lat': location.lat if location else None,
            'country': location.country if location else None,
            'resolution': location.resolution if location else None,
        }

    def descendants(self):
        """Iterate through descendants of person."""
        queue = list(self.children)
        visited = set()
        while queue:
            person = queue.pop()
            if person not in visited:
                yield person
                visited.add(person)
                queue.extend(person.children)

    def walk(self, visitable=None, radius=None):
        """Iterate through neighboring people."""

        if radius:
            if visitable:
                visitable = lambda p: visitable(p) and dists[p] <= radius
            else:
                visitable = lambda p: dists[p] <= radius

        neighbors = self.children + self.parents
        queue = [self] + neighbors
        visited = set()

        # Setup distances.
        dists = {p: 1 for p in neighbors}
        dists[self] = 0

        # Process queue until no more neighbors.
        while queue:
            person = queue.pop()
            if person not in visited and (not visitable or visitable(person)):
                yield person
                visited.add(person)

                neighbors = person.children + person.parents
                dist = dists[person]
                for neighbor in neighbors:
                    dists[neighbor] = min(dists.get(neighbor, dist + 1),
                                          dist + 1)
                queue.extend(neighbors)


def get_graphviz(people, node_style={}):
    ids = {p.id for p in people}

    yield 'digraph graphname {\n'

    # Set node styles.
    for person, param in node_style.iteritems():
        text = ','.join('%s=%s' % (key, value)
                        for key, value in param.iteritems())
        yield '%s [%s];\n' % (person.id, text)

    # Draw edges.
    for person in people:
        for child in person.children:
            if child.id in ids:
                yield '%s -> %s;\n' % (person.id, child.id)
    yield '}\n'


def write_graphviz(filename, people, **kwargs):
    if isinstance(filename, basestring):
        out = open(filename, 'w')
        close = True
    else:
        out = filename
        close = False

    if 'render' in kwargs:
        render = kwargs.pop('render')
    else:
        render = True

    for line in get_graphviz(people, **kwargs):
        out.write(line)

    if close:
        out.close()

        if render:
            base, ext = os.path.splitext(filename)
            pdf_filename = base + '.pdf'
            os.system('dot -o %s -Tpdf "%s"' % (pdf_filename, filename))
