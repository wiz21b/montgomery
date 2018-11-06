import logging
import colorlog
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import ColumnProperty
import typing

def _make_default_logger() -> logging.Logger:
    # Func to hide local values
    default_logger = logging.getLogger( "pyxfer") # FIXME shoudl use __name__, but it returns pyxfer.pyxfer
    log_handler = logging.StreamHandler()
    #log_handler.setFormatter( logging.Formatter("[%(name)s %(asctime)s %(levelname)s] %(message)s"))
    log_handler.setFormatter( colorlog.ColoredFormatter("[%(log_color)s%(name)s %(asctime)s %(levelname)s] %(message)s%(reset)s"))
    default_logger.addHandler( log_handler)
    default_logger.setLevel( logging.CRITICAL + 1) # Hides every logs by default
    return default_logger

_default_logger = _make_default_logger()
# Use this to enable logging in a clean way :
logging.getLogger("pyxfer").setLevel(logging.DEBUG)

_sqla_attribute_analysis_cache = dict()


def merge_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return z

def sqla_attribute_analysis( model, logger : logging.Logger = _default_logger):

    if model in _sqla_attribute_analysis_cache:
        return _sqla_attribute_analysis_cache[model]

    # SQLA column properties --------------------------------------------------

    # Warning ! Some column properties are read only !

    fnames = [prop.key for prop in inspect(model).iterate_properties
              if isinstance(prop, ColumnProperty) ]

    ftypes = dict()
    for fname in fnames:
        t = inspect( getattr(model, fname)).type
        # print( type( inspect( getattr(model, fname)).type))
        ftypes[fname] = type(t)

    # Python properties -------------------------------------------------------

    # Python properties  are hard to handle because we don't know their type.
    # That's because they're out of the SQLAlchemy models (which are strongly
    # typed). The same goes for any method added to SQLA mapper classes.
    # The solution is therefore to somehow tell their type to pyxfer.
    # I thought about using type hints but, as it is stated in the documentation
    # type hints are their to help type verification for code execution,
    # not to be used as a way to do advanced introspection. However, as stated
    # here : https://stackoverflow.com/questions/37913112/how-to-check-type-compatibility-when-using-the-typing-module , one can use bits of the type system.

    python_props = dict()

    for name,value in vars(model).items():
        if isinstance(value, property):

            # Don't forget @property wraps the method it applies
            # to. So one wants the type of the method, one has to look
            # at the method, not the property.

            getter_method = typing.get_type_hints(value.fget)
            if 'return' in getter_method:
                python_props[name] = getter_method['return']
                _default_logger.debug("Analyzed property '{}.{}', its type is '{}'".format( model.__name__, name, python_props[name]))
            else:
                _default_logger.warn("Can't figure out the type of property '{}.{}', I skip it. Use type hints. I examined {}, which is like {}.".format( model.__name__, name, value.fget, typing.get_type_hints(value.fget)))
                # python_props[name] = None

    # SQLA relations ----------------------------------------------------------

    single_rnames = dict()
    rnames = dict()
    for key, relation in inspect(model).relationships.items():

        # From the relation, we retrieve the mapped class, that is
        # the one written by the SQLA schema author (i.e. you :-))
        mapped_class = relation.mapper.class_
        # print( "Mapper class = {}".format(mapped_class))
        # print( relation.argument.class_)
        if relation.uselist == False:
            single_rnames[key] = mapped_class
        else:
            rnames[key] = mapped_class

    # Key fields --------------------------------------------------------------

    # Order is important to rebuild composite keys (I think, not tested so far).
    # See SQLA comment for query.get operation :
    # http://docs.sqlalchemy.org/en/rel_1_0/orm/query.html#sqlalchemy.orm.query.Query.get )
    knames = [key.name for key in inspect(model).primary_key]

    # logger.debug(
    #     "For model {}, I have these attributes : primary keys={}, fields={}, realtionships={} (single: {})".format(
    #         model, knames, fnames, rnames, single_rnames))

    _sqla_attribute_analysis_cache[model] = ( ftypes, rnames, single_rnames, knames, python_props)
    return ( ftypes, rnames, single_rnames, knames, python_props)



def find_sqla_mappers( mapper : 'class'):
    base_mapper_direct_children = [sc for sc in mapper.__subclasses__()]

    d = dict()

    for direct_child in base_mapper_direct_children:
        for c in _find_subclasses( direct_child):
            d[c] = {}

    return d

def _find_subclasses( cls):
    # Handle SQLA inherited entities definitions

    if cls.__subclasses__():
        results = [ cls ]
        for sc in cls.__subclasses__():
            results.extend( _find_subclasses(sc))
        return results
    else:
        return [cls]


class CodeWriter:
    def __init__(self):
        self._code = [] # array of string
        self._indentation = 0

    def indent_right(self):
        self._indentation += 1


    def indent_left(self):
        self._indentation -= 1

        # if len(self._code) >= 1 and self._code[-1]:
        #     self._code.append("")

    def insert_code(self, lines, ndx, indentation_level = 0):
        if type(lines) == str:
            lines = lines.split('\n')
        elif type(lines) == list:
            pass
        elif isinstance( lines, CodeWriter):
            lines = lines._code
        elif lines is None:
            return
        else:
            raise Exception("Unexpected data")

        for i in range( len( lines)):
            line = lines[i]
            self._code.insert( ndx+i, "    " * indentation_level + line)

    def append_code(self, lines):
        self.insert_code(lines, len(self._code), self._indentation)

    def append_blank(self):
        # Avoir double blanks
        if len(self._code) >= 1 and self._code[-1].strip():
            self.append_code("")

    def generated_code(self):
        return "\n".join(self._code)

    def __str__(self):
        return self.generated_code()
