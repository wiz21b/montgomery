import inspect as pyinspect

from sqlalchemy import Integer, String
from sqlalchemy.inspection import inspect

from pyxfer.pyxfer  import _default_logger, TypeSupport, Serializer, CodeWriter, sqla_attribute_analysis



def gen_merge_relation_sqla(serializer : Serializer,
                            relation_source_expr : str,
                            relation_dest_expr : str,
                            rel_source_type_support : TypeSupport,
                            rel_dest_type_support : TypeSupport,
                            serializer_call_code : str,
                            walk_type,
                            collection_class,
                            cache_base_name : str):

    """Generates the code that will read a given relation in a source
    object (accessed via @relation_source_expr) and merge its content
    into a corresponding relation in a destination object accessed via
    @relation_dest_expr). Each item of the collection will be serialized
    individually with code given by @serializer_call_code.

    The objects in the source relation are described by @rel_source_type_support.
    The objects in the destination relation are described by @rel_dest_type_support.

    The type of the relation representation is in @collection_class (set-based
    relations don't work exactly like list-based relations).

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

    #    relation.add( dest2)
    if collection_class == list:
        # serializer.append_code(      'print("{}, {}".format(dest_item2, dest_item2 in session))')
        serializer.append_code(      "{}.append(dest_item2)".format( relation_dest_expr))
    elif collection_class == set:
        #raise Exception("breakpont")
        serializer.append_code(      "{}.add(dest_item2)".format( relation_dest_expr))
    else:
        raise Exception("Unrecognized collection type ({})".format( collection_class))

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

        self.fnames, self.rnames, self.single_rnames, self.knames = sqla_attribute_analysis( self._model)

        fields = dict()
        for f, t in self.fnames.items():

            #print("{} {}".format(f,t))
            if t == String:
                fields[f] = str
            elif isinstance(t, Integer):
                fields[f] = int
        self._fields = fields

    def finish_serializer(self, serializer):
        """ What to do at the end of a serializer which produces instances
        of the type described by this TypeSupport.
        """
        pass


    def check_instance_serializer(self, serializer, dest : str):
        # check if key is not empty
        serializer.append_code( "# Merging into SQLA session. We do that after")
        serializer.append_code( "# having filled all the fields so that")
        serializer.append_code( "# SQLA will copy them efficiently")
        serializer.append_code( "{} = session.merge({})".format( dest, dest))

    def gen_global_code(self) -> CodeWriter:
        cw = CodeWriter()
        cw.append_code("from sqlalchemy.orm.session import Session")
        cw.append_code("def _sqla_session_add( session : Session, inst):")
        cw.append_code("    session.add( inst)")
        cw.append_code("    return inst")

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

    def make_instance_code(self, destination):
        return "{}()".format( self.type_name())

    def fields(self):
        return self._fields.keys()

    def relations(self):
        return self.rnames

    def field_type(self, field_name):
        return self._fields[field_name]

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

    def gen_create_instance(self):
        return "_sqla_session_add( session, {}())".format(self.type_name())

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
        serializer.append_code("# Copy relation '{}'".format(relation_name))
        relation_source_expr = source_ts.gen_read_relation( source_instance_name,relation_name)
        relation_dest_expr = dest_ts.gen_read_relation( dest_instance_name,relation_name)

        #mainlog.debug("{} --> {}".format( source_ts, dest_ts))
        #mainlog.debug(walk_type)

        a = getattr( dest_ts.type(), relation_name).property
        if a.collection_class == set:
            collection_class = set
            #raise Exception("Breakpoint {}, collection_class={}".format(a, a.collection_class))
        else:
            collection_class = list

        return gen_merge_relation_sqla(serializer,
                                       relation_source_expr, relation_dest_expr,
                                       rel_source_type_support, self,
                                       serializer_call_code,
                                       walk_type,
                                       collection_class,
                                       cache_base_name)

    def __str__(self):
        return "SQLATypeSupport[{}]".format( self.type_name())













class DictTypeSupport(TypeSupport):
    def __init__(self, base_type = None):
        # We don't need the base_type
        pass

    def type(self):
        return dict

    def type_name(self):
        return "dict"

    def fields(self):
        return []

    def relations(self):
        return []

    def field_type(self, field_name):
        return str

    def make_instance_code(self, destination):
        return "{}()".format( self.type_name())

    def gen_create_instance(self):
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

    def __str__(self):
        return "DictTypeSupport[{}]".format( self.type_name())




class ObjectTypeSupport(TypeSupport):

    def __init__(self, obj_or_name):
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

    def gen_create_instance(self) -> str:
        return "{}()".format( self.type_name())

    def field_type(self, field_name):
        return str

    def make_instance_code(self, destination):
        return "{}()".format( self.type_name())

    def gen_global_code(self) -> CodeWriter:
        cw = CodeWriter()

        if True or self._base_type is None:
            cw.append_code("class {}:".format(self._name))
            cw.append_code("    def __init__(self):".format(self._name))
            for f in self._fields:
                cw.append_code("        self.{} = None".format(f))

            if self._relations:
                for r,type_ in self._relations.items():
                    cw.append_code("        self.{} = {}".format(r,type_))
            else:
                cw.append_code("        # no relations")

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

        serializer.append_code("# ------ relation : {} ------".format(relation_name))
        serializer.append_code("")
        relation_source_expr = source_ts.gen_read_relation( source_instance_name, relation_name)
        relation_dest_expr = dest_ts.gen_read_relation( dest_instance_name, relation_name)


        serializer.append_code( "{}.clear()".format(relation_dest_expr))
        serializer.append_code( "for item in {}:".format(
            relation_source_expr))
        serializer.indent_right()

        serializer.append_code("{}.append( {})".format(
            relation_dest_expr,
            serializer_call_code('item', None)))

        serializer.indent_left()
        return








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
    """

    REUSE_TAG = "__PYXPTR" # Points to a instance that has an ID
    ID_TAG = "__PYXID" # ID of an instance

    def __init__(self, base_type):
        ftypes, rnames, single_rnames, self._key_names = sqla_attribute_analysis( base_type)

    def cache_key( self, serializer : Serializer, cache_key_var : str, source_instance_name : str, cache_base_name : str):

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

        serializer.append_code( "cache[cache_key] = {{ '{}' : ekey[1:] }}".format(
            self.REUSE_TAG))
        serializer.indent_left()
        serializer.append_code("else:")
        serializer.indent_right()
        serializer.append_code( "# We use a PYXID only if it is necessary, that is, only if")
        serializer.append_code( "# an object has no primary/business key")
        serializer.append_code( "cache[cache_key] = {}".format(
            self._make_cache_value_expression( self._key_names, source_type_support, source_instance_name, [ "'__PYX_REUSE' : id(source)" ])))

        serializer.append_code( "dest['{}'] = id(source)".format( self.ID_TAG))
        serializer.indent_left()


    def _make_cache_value_expression( self, key_fields, type_support : TypeSupport, instance_name, base_parts):
        parts = base_parts
        for k_name in key_fields:
            parts.append( "'{}' : {}".format(
                k_name, type_support.gen_read_field( instance_name, k_name)))

        return "{{ {} }}".format( ",".join( parts))


    def _make_cache_key_expression( self, key_fields, cache_base_name, type_support : TypeSupport, instance_name):
        """ Builds a tuple containing th key fields values
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
