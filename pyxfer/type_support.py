import logging
import typing
import inspect as pyinspect

from sqlalchemy import Integer, String
from sqlalchemy.inspection import inspect

from pyxfer.utils import sqla_attribute_analysis, _default_logger, CodeWriter


class TypeSupport:
    """A type support class has the responsbility to build *code
    fragments* to read/write/create instances of the type it
    supports. It doesn't build code of a full blown serializer (this
    is done by the @Serializer class).

    By "read" we mean, read field values, read relationships, etc.
    Same goes for "write".

    TypeSupport should be reusable for several classes (of the same
    type) that share the TypeSupport class. That is, if one writes a
    SQLAlchemy mapped class TypeSupport, then one should be able to
    reuse that TypeSupport with various classes that are mapped with
    SQLAlchemy. In this case, the TypeSupportFactory base class should
    be considered.

    The TypeSupport can always be specialized for some scenarios. That
    is, if one makes a TypeSupport that handles regular objects, then,
    afterwards, one can make a specific TypeSupport for objects having
    special characteristics.

    The TypeSupport may (must?) have access to the type it supports to
    determine :

    - the type of the fields (for example int, floats, date...)
    - the type of relations (list of OtherClass, set of OtherClass)

    This may conflict with the walker's fields controls. Indeed, if
    a field controls explicitely prevents serialization of a given
    field, one must make sure that the type support knows that. So
    there's some kind of interaction between walker and type supports.

    """

    def __init__( self, logger : logging.Logger = _default_logger):
        self._logger = logger
        self.additional_global_code = CodeWriter()

    def type(self):
        """ The type managed by this TypeSupport.
        """
        raise NotImplementedError()

    def type_name(self) -> str:
        """ The name of the type managed by this TypeSupport.

        This name will be used to build function prototypes, so it
        must be a valid type definition.
        """
        raise NotImplementedError()

    def field_collection_type(self, field_name : str):
        # Returns list or set.
        return list # FIXME Make it a NotImplementedError

    def relation_read_iterator( self, relation_name : str):
        # By default the iterator works over immutable sequence-like objects;

        return self.gen_relation_read_iteration

    def finish_serializer(self, serializer : CodeWriter):
        """ What to do at the end of a serializer which produces instances
        of the type described by this TypeSupport.
        """
        pass

    def check_instance_serializer( self, serializer : CodeWriter, dest_instance_name : str):
        pass

    def cache_key( self, serializer : CodeWriter, key_var : str, source_instance_name : str, cache_base_name : str):
        """ Builds code to compute the key that will be used
        to cache serialization results. If the results must
        not be cached (once or never), the generated expression
        must evaluate to None.
        """

        # Default implementation, may not work for every
        # scenarios (see DictTypeSupport for example).

        serializer.append_code( "{} = (\"{}\", id({}))".format( key_var, cache_base_name, source_instance_name))


    def cache_on_write(self, serializer : CodeWriter, source_type_support, source_instance_name, cache_base_name, dest_instance_name):

        # Default implementation, may not work for every
        # scenarios (see DictTypeSupport for example).

        #serializer.append_code("print('caching : key={{}}, data={{}}'.format(cache_key,{}))".format( dest_instance_name))
        serializer.append_code("cache[cache_key] = {}".format( dest_instance_name))



    def relation_copy(self, serializer : CodeWriter,
                      source_instance_name : str, dest_instance_name : str, relation_name,
                      source_ts, dest_ts,
                      rel_source_type_support,
                      serializer_call_code,
                      walk_type,
                      cache_base_name):

        """ This will return a function that can build the code to serialize
        *to* the relation named @relation_name of an object represented
        by this TypeSupport.

        FIXME This works only on SQLA types, and its not normal to have a
        distinction between sequence_copy and relation_copy at this place
        of the abstraction, this should be done elsewhere.

        To generate the code, we use various informations :
        - relation_name : the name of the relation we copy, we expect that name
          to be the same in the source and destination object.
        - dest_instance_name : the name of the object to which the relation
          will be copied. We will basically generate code that copies to
          "dest_instance_name"."relation_name".
        - source_instance_name : the name of the object from which the relation
          will be copied. We will basically generate code that copies from
          "source_instance_name"."relation_name".
        - dest_ts : the @TypeSupport describing the destination object where we'll
          copy the relation to.
        - source_ts : the @TypeSupport describing the source object where we'll
          read the relation from.
        - rel_source_type_support : the @TypeSupport describing the objects which
          are in the source relation. So we'll make code that transforms
          those objects into the objects of the destination relation.
        - rel_dest_type_support : the @TypeSupport describing the objects which
          are in the destination relation.

        """

        raise NotImplementedError()


    def sequence_copy(self, serializer, source_instance_name, dest_instance_name, relation_name,
                      source_ts, dest_ts,
                      rel_source_type_support,
                      serializer_call_code,
                      base_type, cache_base_name):

        serializer.append_code("# ------ sequence : {} ------".format(relation_name))

        return sequence_copy( serializer, source_instance_name,
                              dest_instance_name, relation_name,
                              source_ts, dest_ts,
                              rel_source_type_support,
                              serializer_call_code,
                              base_type, cache_base_name)


    def gen_write_field(self, instance, field, value) -> str:
        raise NotImplementedError()

    def gen_append_field(self, instance_name :str, field_name : str, expr_to_add : str) -> str:
        t = self.field_collection_type()
        if t == list:
            return "{}.{}.append( {})".format( instance_name, field_name, expr_to_add)
        elif t == set:
            return "{}.{}.add( {})".format( instance_name, field_name, expr_to_add)
        else:
            raise Exception("Unsupported sequence type {} for field {}".format( t, field_name))

    def gen_is_single_relation_present(self, instance, relation_name) -> str:
        """ Returns an expression that evaluate to True if a
        a single item relation (ie a one-to-one relation, for example
        an irder has one customer) is present as a relation
        (e.g. the data of the customer appear inside the
        data of the order, which is not, a customer_id in an
        order object).

        This is used to optimize a situation where one makes
        a reference (using for ex. a customer_id field inside an order)
        instead of a full copy of an object (using for ex. a
        customer object inside an order)

        FIXME This is so unclear I wonder why it's necessary.
        """

        raise NotImplementedError()

    def gen_read_field(self, instance : str, field : str) -> str:
        raise NotImplementedError()

    def gen_read_relation(self, instance : str, relation_name : str) -> str:
        raise NotImplementedError()

    def gen_create_instance(self, key_tuple_code : str) -> str:
        """ Generate code to create a new instance of the supported type.
        Note you can use self.type_name() to get the concrete type. """
        raise NotImplementedError()

    def serializer_additional_parameters(self):
        """ Additional parameters to be passed to the serializer
        so that the code generated by this TypeSupport can be
        use it.

        Returns an array of parameters declaration. For example,
        in SQLAlchemy, [ "session : Session" ] makes sense in a lot
        of places.
        """

        return [] # Default value here because add. params are not frequent.


    def gen_global_code(self) -> CodeWriter:
        return CodeWriter()

    def gen_merge_relation(self, serializer, serializer_call_code, source_instance_name: str, relation_name: str,
                           source_type_support, dest_instance_name : str, dest_rel_name : str):
        raise NotImplementedError()


    def gen_relation_read_iteration(self, serializer, source_instance_name: str, relation_name: str):
        serializer.append_code("for item in {}: # copy from {}".format(
            self.gen_read_relation( source_instance_name, relation_name), self))




class AbstractTypeSupportFactory:
    """ Abstract class to inherit TypeSupport factories from.

    Note that we cache the Typesupport we create.
    """

    def __init__(self, logger : logging.Logger = _default_logger):
        self._logger = logger
        self._supported_types = []
        self._types_support = []


    def get_type_support(self, base_type):
        if base_type not in self._supported_types:
            # self._logger.debug("Factory creates a new type support for {}".format(base_type))

            self._supported_types.append( base_type)
            t = self.make_type_support(base_type)
            self._types_support.append( t)
            return t
        else:
            return self._types_support[ self._supported_types.index( base_type)]

    def make_type_support(self, base_type):
        raise NotImplementedError()


class TypeSupportFactory(AbstractTypeSupportFactory):

    def __init__( self, type_support_class : TypeSupport, logger : logging.Logger = _default_logger):
        assert type( type_support_class) == type
        super().__init__( logger)
        self._type_support_class = type_support_class

    def make_type_support(self, base_type):
        self._logger.debug("TypeSupportFactory : trying to make a '{}' with a '{}'".format(self._type_support_class, base_type))
        return self._type_support_class( base_type)




def sequence_copy( serializer : CodeWriter,
                   source_instance_name : str, dest_instance_name : str, relation_name : str,
                   source_ts : TypeSupport, dest_ts : TypeSupport,
                   rel_source_type_support : TypeSupport,
                   serializer_call_code : str,
                   base_type, cache_base_name : str):

    relation_source_expr = source_ts.gen_read_relation( source_instance_name, relation_name)
    relation_dest_expr = dest_ts.gen_read_relation( dest_instance_name, relation_name)

    serializer.append_code("")
    serializer.append_code( "{}.clear()".format(relation_dest_expr))
    serializer.append_code( "for item in {}:".format(
        relation_source_expr))
    serializer.indent_right()
    serializer.append_code("{}.append( {})".format(
        relation_dest_expr,
        serializer_call_code('item', None)))
    serializer.indent_left()


def gen_merge_relation_sqla(serializer : CodeWriter,
                            relation_source_expr : str,
                            relation_dest_expr : str,
                            rel_source_type_support : TypeSupport,
                            rel_dest_type_support : TypeSupport,
                            serializer_call_code : str,
                            #walk_type,
                            collection_class,
                            cache_base_name : str):

    """Generates the code that will read a given relation in a source
    object (accessed via @relation_source_expr) and merge its content
    into a corresponding relation in a destination object accessed via
    @relation_dest_expr). By merging, we mean that we'll add/remove
    items from the relation in the corresponding object (we won't
    create a brand new relation and stick it in the destination, we'll
    use the one from the destination).

    Each item of the collection will be serialized individually with
    code given by @serializer_call_code.

    The objects in the source relation are described by
    @rel_source_type_support.  The objects in the destination relation
    are described by @rel_dest_type_support.

    The type of the relation representation is in @collection_class
    (set-based relations don't work exactly like list-based
    relations).
    """

    serializer.append_code("# Keep track of added/updated items.")
    serializer.append_code("used = set()")

    serializer.append_code("for item in {}:".format( relation_source_expr))
    serializer.indent_right()

    # To accomplish the merge operation, we will analyse the key-tuples
    # of each object in the source and destination relation.
    # The question is : how do we determine the key tuples.

    rel_source_type_support.cache_key( serializer, "rck", "item", cache_base_name)

    # The question is where do we deserialize.
    # We use SQLA's behaviour indirectly a lot here.
    # That is, SQLA maintains a cache of the objects it knows about
    # and session.add/merge is used to populate that.
    # We need out own cache to be able to translate from
    # surrogate key to instance pointers (for business keys
    # we could've used SQLA query but that would've been longer
    # to code and less efficient IMHO).

    serializer.append_code(   "dest_item = cache.get( rck, None)")

    # One cannot serialize before merging because if one does
    # so, then we may miss instance reuse in the destination
    # relation

    serializer.append_code(   "dest_item2 = {}".format(
        serializer_call_code("item", "dest_item")))

    serializer.append_code(   "if dest_item is None:")
    serializer.indent_right()

    #collection_class = rel_dest_type_support.field_collection_type(
    #    relation.add( dest2)

    # See method is_subtype(...) in :
    # https://github.com/python/mypy/blob/master/mypy/subtypes.py

    if collection_class == list or issubclass(collection_class, typing.List):
        # serializer.append_code(      'print("{}, {}".format(dest_item2, dest_item2 in session))')
        serializer.append_code(      "{}.append(dest_item2)".format( relation_dest_expr))
    elif collection_class == set:
        #raise Exception("breakpont")
        serializer.append_code(      "{}.add(dest_item2)".format( relation_dest_expr))
    else:
        raise Exception("Unrecognized collection type ({}) while building code to copy relation from '{}' to '{}'".format( collection_class, rel_source_type_support.type_name(), rel_dest_type_support.type_name()))

    serializer.indent_left()

    serializer.append_code(   "used.add( dest_item2)")
    serializer.indent_left()

    serializer.append_code("# What's not marked as added/updated is deleted")
    serializer.append_code("for item in set({}) - used:".format(relation_dest_expr))
    serializer.indent_right()
    serializer.append_code("{}.remove(item)".format(relation_dest_expr))
    serializer.indent_left()









class SQLATypeSupport(TypeSupport):
    def __init__(self, sqla_model):
        self._model = sqla_model

        self.fnames, self.rnames, self.single_rnames, self.knames, self.props = sqla_attribute_analysis( self._model)

        fields = dict()
        for f, t in self.fnames.items():

            #print("{} {}".format(f,t))
            if t == String:
                fields[f] = str
            elif isinstance(t, Integer):
                fields[f] = int
        self._fields = fields

    def field_collection_type(self, field_name : str):

        if field_name in self.rnames:
            a = getattr( self.type(), field_name).property
            if a.collection_class == set:
                return set
            elif a.collection_class == list or a.collection_class == None:
                return list
            else:
                raise Exception("Unrecognized collection type '{}' for a relation {}.{}".format(a.collection_class, self.type_name(), field_name))

        elif field_name in self.props:
            return self.props[field_name]
        else:
            raise Exception("Field {}.{} is not known as a collection".format( self.type_name(), field_name))



    def check_instance_serializer(self, serializer, dest : str):
        # check if key is not empty
        serializer.append_code( "# Merging into SQLA session. We do that after")
        serializer.append_code( "# having filled all the fields so that")
        serializer.append_code( "# SQLA will copy them efficiently")
        serializer.append_code( "if {} not in session:".format(dest))
        serializer.append_code( "    {} = session.merge({})".format(dest, dest))

    def gen_global_code(self) -> CodeWriter:
        cw = CodeWriter()

        cw.append_code("from sqlalchemy.orm.session import Session")
        cw.append_code("def _sqla_session_add( session : Session, klass, kt):")
        cw.append_code("   from sqlalchemy import inspect")
        cw.append_code("   key = inspect( klass).identity_key_from_primary_key( kt )")
        cw.append_code("   if key in session.identity_map:")
        cw.append_code("       return session.identity_map[key]")
        cw.append_code("   else:")
        # will be merged later
        cw.append_code("       dest = klass()")
        cw.append_code("       return dest")

        cw2 = CodeWriter()

        m = pyinspect.getmodule( self._model)
        if m.__name__ == "__main__":
            # the start module is named __main__ and a class ABC
            # defined there is of type : __main__.ABC (even if
            # the module is actually named "test", so you don't
            # get test.ABC but __main__.ABC, weird IMHO).
            package = "__main__"
        elif m and m.__spec__:
            package = m.__spec__.name
        else:
            package = m.__file__.replace(".py","")
            _default_logger.warning("I can't find the package name, did you run a python file instead of a python moduyle (python -m ...)")

        cw2.append_code( "from {} import {}".format( package, self._model.__name__))
        return [cw, cw2]

    def type(self):
        return self._model

    def type_name(self):
        return self._model.__name__


    def relations(self):
        return self.rnames

    def field_read_code(self, repr, field_name):
        return "{}.{}".format(repr, field_name)

    def gen_is_single_relation_present(self, instance, relation_name) -> str:
        return "{}.{}".format( instance, relation_name)

    def relation_write_code(self, expression, relation_name, source_walker):
        return "{}.{} = map( serialize_relation_from_{}, {})".format( "zzz",relation_name, source_walker.type_name(), expression)

    def gen_write_field(self, instance, field, value):
        return "{}.{} = {}".format( instance, field, value)

    def gen_basetype_to_type_conversion(self, field, code):
        return "( {})".format(code)

    def gen_read_field(self, instance, field):
        return "{}.{}".format(instance, field)

    def gen_type_to_basetype_conversion(self, field, code):
        return code

    def gen_init_relation(self, dest_instance, dest_name, read_rel_code):
        return "{}.{} = []".format(dest_instance, dest_name)

    def gen_read_relation( self, instance, relation_name):
        return "{}.{}".format(instance, relation_name)

    def make_instance_code(self, destination):
        return "{}()".format( self.type_name())

    def gen_create_instance(self, key_tuple_code) -> str:
        return "_sqla_session_add( session, {}, {})".format(self.type_name(), key_tuple_code)

    def serializer_additional_parameters(self):
        return ["session : Session"]

    def relation_copy(self, serializer,
                      source_instance_name, dest_instance_name, relation_name,
                      source_ts, dest_ts,
                      rel_source_type_support,
                      serializer_call_code,
                      walk_type,
                      cache_base_name):

        serializer.append_blank()
        serializer.append_code("# SQLATS Copy relation '{}'".format(relation_name))
        relation_source_expr = source_ts.gen_read_relation( source_instance_name,relation_name)
        relation_dest_expr = dest_ts.gen_read_relation( dest_instance_name,relation_name)

        _default_logger.debug("{} --> {}".format( source_ts, dest_ts))
        #_default_logger.debug(walk_type)
        _default_logger.debug(dest_ts.type())

        # a = getattr( dest_ts.type(), relation_name).property
        # if a.collection_class == set:
        #     collection_class = set
        #     #raise Exception("Breakpoint {}, collection_class={}".format(a, a.collection_class))
        # else:
        #     collection_class = list

        collection_class = dest_ts.field_collection_type(relation_name)

        return gen_merge_relation_sqla(serializer,
                                       relation_source_expr, relation_dest_expr,
                                       rel_source_type_support, self,
                                       serializer_call_code,
                                       #walk_type,
                                       collection_class,
                                       cache_base_name)


    def sequence_copy(self, serializer, source_instance_name, dest_instance_name, relation_name,
                      source_ts, dest_ts,
                      rel_source_type_support,
                      serializer_call_code,
                      base_type, cache_base_name):

        return sequence_copy( serializer, source_instance_name,
                              dest_instance_name, relation_name,
                              source_ts, dest_ts,
                              rel_source_type_support,
                              serializer_call_code,
                              base_type, cache_base_name)

    def __str__(self):
        return "SQLATypeSupport[{}]".format( self.type_name())













class DictTypeSupport(TypeSupport):
    """ A type support to represent entities as dictionaries.
    """

    def __init__(self, base_type = None):
        # We don't need the base_type
        pass

    def type(self):
        return dict

    def type_name(self):
        return "dict"

    def relations(self):
        return []

    def make_instance_code(self, destination):
        return "{}()".format( self.type_name())

    def gen_create_instance(self, key_tuple_code) -> str:
        return "{}()".format( self.type_name())

    def field_read_code(self, expression, field_name):
        """ Generates a piece of code to access the field
        named filed_name from an expression of type
        dict.
        """
        return "{}['{}']".format(expression, field_name)

    def gen_write_field(self, instance, field, value):
        return "{}['{}'] = {}".format(instance, field, value)

    def gen_basetype_to_type_conversion(self, field, code):
        return "{}".format( code)

    def gen_read_field(self, instance, field):
        return "{}['{}']".format(instance, field)

    def gen_type_to_basetype_conversion(self, field, code):
        return "{}".format(code)

    def gen_read_relation(self, instance, relation_name):
        return "{}['{}']".format(instance, relation_name)

    def gen_is_single_relation_present(self, instance, relation_name) -> str:
        return "('{}' in {} and {}['{}'] is not None)".format(relation_name, instance, instance, relation_name)

    def relation_copy(self, serializer, source_instance_name, dest_instance_name, relation_name,
                      source_ts, dest_ts,
                      rel_source_type_support,
                      serializer_call_code,
                      base_type, cache_base_name):

        serializer.append_code("# ------ relation : {} ------".format(relation_name))
        serializer.append_code("")
        relation_source_expr = source_ts.gen_read_relation( source_instance_name, relation_name)
        relation_dest_expr = dest_ts.gen_read_relation( dest_instance_name, relation_name)

        serializer.append_code( "{} = []".format(relation_dest_expr))
        serializer.append_code( "for item in {}:".format(
            relation_source_expr))
        serializer.indent_right()

        serializer.append_code("{}.append( {})".format(
            relation_dest_expr,
            serializer_call_code('item', None)))

        serializer.indent_left()
        return

    def sequence_copy(self, serializer, source_instance_name, dest_instance_name, relation_name,
                      source_ts, dest_ts,
                      rel_source_type_support,
                      serializer_call_code,
                      base_type, cache_base_name):

        relation_source_expr = source_ts.gen_read_field( source_instance_name, relation_name)
        relation_dest_expr = dest_ts.gen_read_field( dest_instance_name, relation_name)

        serializer.append_code("# ------ sequence : {} ------".format(relation_name))
        serializer.append_code("")

        # Code replicated here to make sure I set the "[]" entry in the dict
        # rather than trying to clear() it (when obvioulsy it's not there).
        serializer.append_code( "{} = []".format(relation_dest_expr))

        serializer.append_code( "for item in {}:".format(
            relation_source_expr))
        serializer.indent_right()
        serializer.append_code("{}.append( {})".format(
            relation_dest_expr,
            serializer_call_code('item', None)))
        serializer.indent_left()


    def __str__(self):
        return "DictTypeSupport[{}]".format( self.type_name())




class ObjectTypeSupport(TypeSupport):
    """ A type support for plain python object.
    """

    def __init__(self, obj_or_name):

        assert obj_or_name

        super().__init__()

        self._fields = set()
        self._relations = dict()

        if isinstance( obj_or_name, str):
            self._name = obj_or_name
            self._base_type = None
        else:
            #print("Breakpoint : {} aka {}".format( obj_or_name, obj_or_name.__name__))
            self._name = "Copy" + obj_or_name.__name__
            self._base_type = obj_or_name

    def type(self):
        return object

    def type_name(self):
        return self._name

    def gen_create_instance(self, key_tuple_code) -> str:
        return "{}()".format( self.type_name())

    def gen_global_code(self) -> CodeWriter:
        cw = CodeWriter()

        cw.append_code("class {}:".format(self._name))

        cw.indent_right()
        cw.append_code( self.additional_global_code)

        cw.append_code("def __init__(self):".format(self._name))
        for f in self._fields:
            cw.append_code("    self.{} = None".format(f))

        if self._relations:
            for r,type_ in self._relations.items():
                cw.append_code("    self.{} = {}".format(r,type_))
        else:
            cw.append_code("    # no relations")

        cw.indent_left()
        return cw

    def gen_write_field(self, instance, field, value):
        self._fields.add( field)
        return "{}.{} = {}".format(instance, field, value)

    def gen_basetype_to_type_conversion(self, field, code):
        return "{}".format( code)

    def gen_read_field(self, instance, field):
        self._fields.add( field)
        return "{}.{}".format(instance, field)

    def gen_type_to_basetype_conversion(self, field, code):
        return "{}".format(code)

    def gen_is_single_relation_present(self, instance, relation_name) -> str:
        return "{}.{}".format( instance, relation_name)

    def gen_read_relation(self, instance, relation_name):
        self._relations[ relation_name] = '[]'
        return "{}.{}".format(instance, relation_name)

    def relation_copy(self, serializer, source_instance_name, dest_instance_name, relation_name,
                      source_ts, dest_ts,
                      rel_source_type_support,
                      serializer_call_code,
                      base_type, cache_base_name):

        serializer.append_code("# ------ Relation as sequence : {} ------".format(relation_name))

        return sequence_copy( serializer, source_instance_name,
                              dest_instance_name, relation_name,
                              source_ts, dest_ts,
                              rel_source_type_support,
                              serializer_call_code,
                              base_type, cache_base_name)

        # serializer.append_code("# ------ relation : {} ------".format(relation_name))
        # serializer.append_code("")
        # relation_source_expr = source_ts.gen_read_relation( source_instance_name, relation_name)
        # relation_dest_expr = dest_ts.gen_read_relation( dest_instance_name, relation_name)


        # serializer.append_code( "{}.clear()".format(relation_dest_expr))
        # serializer.append_code( "for item in {}:".format(
        #     relation_source_expr))
        # serializer.indent_right()

        # serializer.append_code("{}.append( {})".format(
        #     relation_dest_expr,
        #     serializer_call_code('item', None)))

        # serializer.indent_left()
        # return

    def __str__(self):
        return "ObjectTypeSupport[{}]".format( self.type_name())







class SQLADictTypeSupport(DictTypeSupport):
    """A DictTypeSupport that is tailored to work with SQLAlchemy's entities.

    The challenge we solve here is this. When converting from entities
    to dicts, if the same entity appears more than once, then we don't
    want to have as many dicts representing the same entity. What we
    want is to have one dict as a serialization of the entity
    and as many "shortcuts" dicts to represent it in a short form.

    Let's take an example. We have 3 orders (entity Order, with
    primary key order_id, relationship to Customer) and one customer
    (entity Customer, pk customer_id). In that case, when we'll
    serialize the orders, we'll have :

    * order 1 : { order_id:1, customer_id : 1, customer : { customer_id:10, name : "OnoSendai"}}
    * order 2 : { order_id:2, customer_id : 1, customer : { customer_id:10}}
    * order 3 : { order_id:3, customer_id : 1, customer : { customer_id:10}}

    So we spare the name fields (and any other).

    Doesn't work. Serilialize three orders which are linked to 2 new customers :
    * order 1 : { order_id:1, customer_id : None, customer : { customer_id:None, name : "OnoSendai"}}
    * order 2 : { order_id:2, customer_id : None, customer : { customer_id:None, name : "TessierAshpool"}}
    * order 3 : { order_id:1, customer_id : None, customer : { customer_id:None}}

    what is the customer of the third order OnoSendai or TessierAshpool ? Even if I do :
    * order 1 : { order_id:1, customer_id : None, customer : { customer_id:None, name : "OnoSendai"}}
    * order 2 : { order_id:2, customer_id : None, customer : { customer_id:None, name : "TessierAshpool"}}
    * order 3 : { order_id:1, customer_id : None, customer : { customer_id:None, name : "OnoSendai"}}

    I Still have the problem that the same customer will appear twice, I won't be able
    to figure out there are only 2 Customers...

    So the easiest way to do that is to attribute a special key to
    every object we serialize to dict. One could think about using the
    actual business key (like the primary key for SQLA entities), but
    that's not a good idea because in case of brand new objects, that
    key may not be initialized, and therefore, if we have 3 new
    objects of one type, then they'll have 3 times the same business
    key, and therefore, they'll be undistinguishable.

    There's one more gotcha though. The caching mechanism relies on
    perfect symmetry between read and write. That is, when one writes
    dict's with this TypeSupport, some special keys are produced to avoid
    dict duplication (our goal). However, those keys make sense to
    the *reader* when they are read in the same order they were produced.
    That is, they must be read after the dict they duplicate has been
    read so that the cache is initialized properly. This is important
    to keep in my mind when one wants to build serializer which are
    not symmetric, that is, serializer that read/write more than their
    deserializer will write/read.
    """

    REUSE_TAG = "__PYXPTR" # Points to a instance that has an ID
    ID_TAG = "__PYXID" # ID of an instance

    def __init__(self, base_type):
        ftypes, rnames, single_rnames, self._key_names, props = sqla_attribute_analysis( base_type)

    def cache_key( self, serializer : CodeWriter, cache_key_var : str, source_instance_name : str, cache_base_name : str):

        # Compute cache key out of a dict
        cke = self._make_cache_key_expression( self._key_names, cache_base_name, self, source_instance_name)

        # With dicts, we can't rely on PK/busineesKeys because when
        # they are not set, we can't detect multiple appearance of
        # the same object. So we use surrogate keys.

        serializer.append_code("if '{}' in {}:".format( self.REUSE_TAG, source_instance_name))
        serializer.indent_right()
        serializer.append_code("v = {}['{}']".format( source_instance_name, self.REUSE_TAG))

        # When deserializing to dict, sometimes a tuple becomes
        # array (for exampe, python's json module does that).
        # So I bring everything back to a tuple. I put list first
        # because that's the most common case.
        serializer.append_code("if type(v) in (list, tuple):".format( source_instance_name))
        serializer.indent_right()
        serializer.append_code("{} = ('{}',) + tuple(v)".format(
            cache_key_var,
            cache_base_name))
        serializer.indent_left()

        # If not a tuple, it can be an int, a float, a string, whatever...
        serializer.append_code("else:")
        serializer.indent_right()
        serializer.append_code("{} = ('{}', v)".format(
            cache_key_var,
            cache_base_name))
        serializer.indent_left()

        serializer.indent_left()
        serializer.append_code("elif '{}' in {}:".format( self.ID_TAG, source_instance_name))
        serializer.indent_right()
        serializer.append_code("# This one will be *stored* in cache")
        serializer.append_code("{} = ('{}', {}['{}'])".format(
            cache_key_var,
            cache_base_name,
            source_instance_name,
            self.ID_TAG))
        serializer.indent_left()
        serializer.append_code("else:")
        serializer.indent_right()

        key_parts_extractors = []
        for k_name in self._key_names:
            key_parts_extractors.append(
                self.gen_read_field( source_instance_name, k_name))

        serializer.append_code("{} = ('{}', {})".format( cache_key_var, cache_base_name, ",".join(key_parts_extractors)))

        serializer.indent_left()


    def cache_on_write(self, serializer, source_type_support, source_instance_name, cache_base_name, dest_instance_name):
        cke = self._make_cache_key_expression( self._key_names, cache_base_name, source_type_support, source_instance_name)
        serializer.append_code("ekey = {}".format(cke))

        serializer.append_code("# Compute how the object will be rendered if it's to be")
        serializer.append_code("# rendered again")
        serializer.append_code("if any( ekey[1:]):")
        serializer.indent_right()

        # Remember __PYX_REUSE is only set for objects wihout PK. In
        # case the PK is not set for two objects of same type, then
        # SQLA (or any other deserializer) has no way to know that
        # they are both the same objects or they are different
        # objects. The __PYX_REUSE allows to remove this unknown.

        serializer.append_code( "# We use the SQLA key because it's set.")
        serializer.append_code( "cache[cache_key] = {{ '{}' : ekey[1:] }}".format(
            self.REUSE_TAG))
        serializer.indent_left()
        serializer.append_code("else:")
        serializer.indent_right()
        serializer.append_code( "# No key set, we use id() to build one.")
        serializer.append_code( "cache[cache_key] = {{ '{}' : id(source) }}".format( self.REUSE_TAG))

        serializer.append_code( "# We use a {} only if it is necessary, that is, only if".format(self.ID_TAG))
        serializer.append_code( "# an object has no primary/business key")
        serializer.append_code( "dest['{}'] = id(source)".format( self.ID_TAG))
        serializer.indent_left()


    def _make_cache_key_expression( self, key_fields, cache_base_name, type_support : TypeSupport, instance_name):
        """ Builds a tuple containing the key fields values
        """
        assert type(key_fields) == list and len(key_fields) > 0, "Wrong keys : {}".format( key_fields)
        assert isinstance( type_support, TypeSupport)
        assert type(instance_name) == str and len(instance_name) > 0

        key_parts_extractors = []
        if cache_base_name:
            key_parts_extractors.append( "'{}'".format(cache_base_name))

        for k_name in key_fields:
            key_parts_extractors.append( type_support.gen_read_field( instance_name, k_name))

        return "({},)".format( ",".join(key_parts_extractors))
