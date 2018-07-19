import inspect as pyinspect

from sqlalchemy import Integer, String
from sqlalchemy.inspection import inspect

from montgomery.montgomery  import default_logger, TypeSupport, Serializer, CodeWriter, sqla_attribute_analysis




def gen_merge_relation_sqla(serializer : Serializer,
                            relation_source_expr : str,
                            relation_dest_expr : str,
                            rel_source_type_support : TypeSupport,
                            rel_dest_type_support : TypeSupport,
                            serializer_call_code,
                            walk_type,
                            collection_class=list):

    """ Generates the code that will read a given relation in a source
    object (@source_ts) and merge its content into a corresponding relation in a
    destination object @dest_ts).

    The objects in the source relation are described by @rel_source_type_support.
    The objects in the destination relation are described by @rel_dest_type_support.
    """

    assert isinstance(rel_source_type_support, TypeSupport)
    assert isinstance(rel_dest_type_support, SQLATypeSupport), "This merge operation will merge *to* some SQLA relations *only*"

    # To accomplish the merge operation, we will analyse the key-tuples
    # of each object in the source and destination relation.
    # The question is : how do we determine the key tuples. Shall
    # we look into the @rel_source_type_support or @rel_dest_type_support ?

    ftypes, rnames, single_rnames, knames = sqla_attribute_analysis( walk_type)

    # if relation_name not in rnames:
    #     mainlog.error("While generating code to merge from '{}.{}' to '{}.{}'".format(source_instance_name, relation_name, dest_instance_name, relation_name ))
    #     raise Exception( "Missing '{}' in '{}' (available values are {}))".format( relation_name, rel_dest_type_support.type(),  ", ".join(rnames.keys())))

    mapper = inspect( walk_type) # rel_dest_type_support.type()

    k_names = [k.name for k in mapper.primary_key]

    key_parts_extractors = []
    source_key_parts_extractors = []
    for k_name in k_names:
        # Now we need to access the key fields of the members of the relationship :
        # - in the source type
        # - in the destination type
        # At this point we have relation_serializer which can serialize members of the relationship.
        # It aldo knows about its source and destination type supports.

        key_parts_extractors.append(          rel_dest_type_support.gen_read_field("item", k_name))
        source_key_parts_extractors.append( rel_source_type_support.gen_read_field("item", k_name))

    # One cannot serialize before merging because if one does
    # so, then we may miss instance reuse in the destination
    # relation

    # We base ourselves on the destination relation because
    # in this one there are no item without keys. So the code
    # is a bit simpler to write.

    serializer.append_code("dest_inst_keys = dict()")
    serializer.append_code("for item in {}:".format( relation_dest_expr))
    serializer.append_code("   dest_inst_keys[ ({})] = {} # from {}".format(",".join(key_parts_extractors), "item", rel_dest_type_support))

    # At this point we have a dict of all the existing keys.
    # We can now write the code to merge.

    # We run through the source items. Each one is either
    # added or merged.

    serializer.append_code("for item in {}:".format( relation_source_expr))
    serializer.append_code("   key = {}   # from {}".format(",".join(source_key_parts_extractors), rel_source_type_support))
    serializer.append_code("   if key in dest_inst_keys:")
    serializer.append_code("       # merge into existing destination instance already in the SQLA session")
    serializer.append_code("       {}".format(serializer_call_code("item", "dest_inst_keys[key]")))
    serializer.append_code("       del dest_inst_keys[key] # Mark the key as treated (by removing it) ")
    serializer.append_code("   else:")
    serializer.append_code("       # adding new destination instance to the SQLA session")
    serializer.append_code("       # this will create new dest instances if necessary ")
    serializer.append_code("       s = {}".format(serializer_call_code("item", None)))
    serializer.append_code("       session.add(s)")



    if collection_class == list:
        serializer.append_code("       {}.append({}) # Container is a list".format( relation_dest_expr, "s"))
    elif collection_class == set:
        #raise Exception("breakpont")
        serializer.append_code("       {}.add({}) # Container is a set ".format( relation_dest_expr, "s"))
    else:
        raise Exception("Unrecognized collection type")


    serializer.append_code("# Remove objects in dest but not in source (we assume they're deleted)")
    serializer.append_code("for inst in dest_inst_keys.values():")
    serializer.append_code("   {}.remove(inst)".format( relation_dest_expr))
    serializer.append_blank()



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
        if m and m.__spec__:
            package = m.__spec__.name
        else:
            package = m.__file__.replace(".py","")
            default_logger.warning("I can't find the package name, did you run a python file instead of a python moduyle (python -m ...)")

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


    # def relation_iterator_code(self, expression, relation_name):
    #     return "{}.{}.iterator()".format(expression, relation_name)

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
                      walk_type):

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
                                       collection_class)


    def __str__(self):
        return "SQLATypeSupport[{}]".format( self.type_name())













class DictTypeSupport(TypeSupport):
    def __init__(self, base_type = None):
        # We don't need the base_type
        pass
        # self._lines=[]

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


    # def gen_init_relation(self, dest_instance, dest_name, read_rel_code):
    #     return "{}['{}'] = []".format(dest_instance, dest_name)

    def gen_read_relation(self, instance, relation_name):
        return "{}['{}']".format(instance, relation_name)

    def gen_is_single_relation_present(self, instance, relation_name) -> str:
        return f"('{relation_name}' in {instance} and {instance}['{relation_name}'] is not None)"

    def relation_copy(self, serializer, source_instance_name, dest_instance_name, relation_name,
                      source_ts, dest_ts,
                      rel_source_type_support,
                      serializer_call_code,
                      base_type = None):

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
            self._name = obj_or_name.__name__
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


    # def relation_iterator_code(self, expression, relation_name):
    #     return "{}['{}'].iterator()".format(expression, relation_name)


    # def write_serializer_to_self_head(self, source, out_lines):
    #     """ Creates a function that will read a source
    #     object and serializes its content to a dict.

    #     :param source:
    #     :return:
    #     """

    #     #out_lines.append("def serialize_relationship_from_dict(d : dict)")
    #     out_lines.append("def serialize( source : {}, dest : {}):".format(  source.type_name(), self.type_name()))
    #     #out_lines.append("    d = dict()")

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


    # def gen_init_relation(self, dest_instance, dest_name, read_rel_code):
    #     self._relations[ relation_name] = '[]'
    #     return "{}.{} = []".format(dest_instance, dest_name)
    def gen_is_single_relation_present(self, instance, relation_name) -> str:
        return "{}.{}".format( instance, relation_name)

    def gen_read_relation(self, instance, relation_name):
        self._relations[ relation_name] = '[]'
        return "{}.{}".format(instance, relation_name)

    def relation_copy(self, serializer, source_instance_name, dest_instance_name, relation_name,
                      source_ts, dest_ts,
                      rel_source_type_support,
                      serializer_call_code,
                      base_type = None):

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

    def __init__(self, base_type = None):
        ftypes, rnames, single_rnames, self._key_names = sqla_attribute_analysis( base_type)

    def cache_key( self, serializer : Serializer, key_var : str, source_instance_name : str, cache_base_name : str):
        serializer.append_code("{} = {}".format(
            key_var,
            self._make_cache_key_expression( self._key_names, cache_base_name, self, source_instance_name)))
        serializer.append_code("if not any( {}[1:]):".format( key_var))
        serializer.indent_right()
        serializer.append_code("{} = None".format( key_var))
        serializer.indent_left()

    def cache_on_write(self, serializer, source_type_support, source_instance_name, cache_base_name, dest_instance_name):
        serializer.append_code("cache[cache_key] = {}".format(
            self._make_cache_value_expression( self._key_names, source_type_support, source_instance_name) ))


    def _make_cache_value_expression( self, key_fields, type_support : TypeSupport, instance_name):
        parts = []
        for k_name in key_fields:
            parts.append( "'{}' : {}".format(
                k_name, type_support.gen_read_field( instance_name, k_name)))

        return "{{ {} }}".format( ",".join( parts))


    def _make_cache_key_expression( self, key_fields, cache_base_name, type_support : TypeSupport, instance_name):
        assert type(key_fields) == list and len(key_fields) > 0, "Wrong keys : {}".format( key_fields)
        assert isinstance( type_support, TypeSupport)
        assert type(instance_name) == str and len(instance_name) > 0

        key_parts_extractors = [ "'{}'".format(cache_base_name)]
        for k_name in key_fields:
            key_parts_extractors.append( type_support.gen_read_field( instance_name, k_name))

        return "({})".format( ",".join(key_parts_extractors))
