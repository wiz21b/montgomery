import logging
from datetime import datetime
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import ColumnProperty
from pyxfer.utils import sqla_attribute_analysis, _default_logger,  CodeWriter, merge_dicts
from pyxfer.type_support import TypeSupport, TypeSupportFactory

SKIP = "!skip"
COPY = "copy"
USE = "use"
CLEAR_APPEND = "by append"
REPLACE = "by index"
FACTORY = "FACTORY"
LIST = "LIST"



def make_cache_base_name( source_ts : TypeSupport, dest_ts : TypeSupport):
    return "{}_{}".format( source_ts.type_name(), dest_ts.type_name())



def extract_key_tuple( key_fields, cache_base_name, type_support : TypeSupport, instance_name):
    """Build code that builds a tuple containing the key fields values of
    an instance

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



class Serializer(CodeWriter):
    """ Holds the code that will implement a serializer. The code kept by the
    serializer is a function that makes a @dest_type out of a @start_type.
    This class is merely a container for that code. The code production
    is dictated by the Walker and the TypeSupports of the start_type
    and dest_types.

    A serializer has the responsibility to create an instance
    of its destination type if none is given to him. Therefore
    it must return that instance to the caller.

    The function prototype is built here, but its body is built elsewhere,
    while *walking* @base_type_name.
    """

    def __init__(self, start_type : TypeSupport, base_type_name : str, dest_type : TypeSupport, serializer_name = None,
                 additional_parameters = []) :
        """
        The method @call_code gives a code fragment to call this serializer (which is
        necessary to allow to call one serializer from another)

        :param start_type: The type this serilaizer will read from
        :param base_type_name: The schema type that will structure the serializer.
        :param dest_type:The type this serilaizer will right to
        :param serializer_name: Optional seralizer name. Change this to make the
               generated code more readable to you.
        :param additional_parameters: Additional parameters are *requested* by
              the serializer. For example, this can be a SQLAlchemy session.
              Note that a serializer may call other serializers. In that
              case the called serializer may require an additional parameter.
              Consequently, the calling serializer must also require the
              additional parameter.
              We could have use globals for that. But globals are usually
              not a good idea (imagine we have two simultaneous SQLA sessions)
              and I prefer functional style, even if it means pushing/popping
              parameters on the stack (which can have an impact on performance).
        """
        super().__init__()

        self.source_type_support, self.base_type_name, self.destination_type_support = start_type, base_type_name, dest_type
        self._name = serializer_name
        self._additional_parameters = additional_parameters
        self._key = None

        # Generate what can be generated right now. The body of the serializer
        # will be generated elsewhere.

        self._proto_serializer()
        self.indent_right()


    def func_name(self):
        """ Builds the name of the function that will hold the serializer
        """

        # Read this like this : following a schema {}, transform
        # an instance of type {} into an instance of type {}

        n = "serialize_{}_{}_to_{}".format(
            self.base_type_name,
            self.source_type_support.type_name(),
            self.destination_type_support.type_name())

        if self._name:
            n += "_" + self._name

        return n

    def call_code(self, additional_parameters = []):
        """ Builds the code fragment that will call the serializer.
        It's used when we navigate recursively in the serialized
        type.
        """

        if not additional_parameters:
            # func_name ( source_inst, dest_inst )
            return "{}({{}}, {{}}, cache)".format( self.func_name()).format
        else:
            return "{}({{}}, {{}}, {}, cache)".format(self.func_name(), ",".join( [ p.split(':')[0] for p in additional_parameters])).format

    def _proto_serializer(self):

        if self._additional_parameters:
            addp = ", ".join(self._additional_parameters) + ","
        else:
            addp = ""

        self.append_code("def {}( source : {}, destination : {}, {} cache : dict):".format(
            self.func_name(),
            self.source_type_support.type_name(),
            self.destination_type_support.type_name(),
            addp))

        self.indent_right()
        self.append_code("if source is None:")
        self.indent_right()
        self.append_code("return None")
        self.indent_left()
        # self.append_code("is_new_instance = dest == None")
        self.indent_left()

    def instance_mgmt(self, knames, source_type_support, dest_type_support):
        # We serialize into a new object if no destination is passed.
        # That's practical : sometimes we don't want to explicitely
        # define what to serialize to. We just expect "serialize(X)" to give
        # us a serialized thing, such as a dict.

        kt = extract_key_tuple( knames, None, source_type_support, "source")
        make_instance = self.destination_type_support.gen_create_instance( kt)
        if make_instance:
            self.append_code( "if destination is None:")
            self.indent_right()
            self.append_code(    "dest = {}".format( make_instance))
            self.indent_left()
            self.append_code( "else:")
            self.indent_right()
            self.append_code(    "dest = destination")
            self.indent_left()
        else:
            self.append_code(    "dest = destination")










class SQLAWalker:
    """ The SQLAWalker is the entry point in the serializer code generation.

    It will walk your SQLA mapper and produce a Serializer
    which will hold the code to your serializer. The walked type
    will be used to guide the code generation. Besides, the code that
    will actually be generated has two sides. The source type support
    which indicates in which class to find the data it needs and
    the destination source type which indicates where to write that
    data. In a sentence,

    The SQLAWalker walks one type at a time. So if you have a network of classes,
    then you'll have to call this for each of these.


    """

    def __init__(self, logger : logging.Logger = _default_logger):
        self._logger = logger

    def _field_copy( self, serializer : Serializer,
                     source_type_support : TypeSupport, source_instance : str,
                     dest_type_support : TypeSupport, dest_instance : str,
                     fields_names):

        for field in sorted( fields_names):

            read_field_code = source_type_support.gen_read_field
            conversion_out_code = source_type_support.gen_type_to_basetype_conversion
            conversion_in_code = dest_type_support.gen_basetype_to_type_conversion
            write_field_code = dest_type_support.gen_write_field

            field_transfer = \
                write_field_code(
                    dest_instance,
                    field,
                    conversion_in_code(
                        field,
                        conversion_out_code(
                            field,
                            read_field_code( source_instance, field))))

            serializer.append_code(field_transfer)


    def gen_type_to_basetype_conversion(self, source_type, base_type):
        if source_type == String and base_type == str:
            return "{}".format



    def walk(self, source_type_support : TypeSupport,
             base_type,
             dest_type_support : TypeSupport,
             fields_control = {}, serializer_name : str = None) -> Serializer:

        """Creates code to copy from a source type to a dest type, following
        a plan dictated by a base_type and fields_control.  Actually
        creates a Serializer which will be able to produce the code.
        The base type is used to guide the walking process.

        We *do not* recurse through the relations.

        Fields controls indicate what to to with each field.
        There are few possibilities :

        * SKIP : the field won't be serialized at all.
        * a Serializer object : the field will be serialized by calling
          the provided serializer (remember serializer represents function).
          In this case, the field is expected to be a (SQLAlhemy) relationship.

        """

        serializers = {}

        if source_type_support is None:
            source_type_support = self.source_factory.get_type_support( base_type)

        if dest_type_support is None:
            dest_type_support = self.dest_factory.get_type_support( base_type)

        assert isinstance(source_type_support, TypeSupport), "Wrong type {}".format( type( source_type_support))
        assert isinstance(dest_type_support, TypeSupport), "Wrong type {}".format( type( dest_type_support))
        assert hasattr(base_type,"__mapper__"), "Expecting SQLAlchemy mapped type"

        # self._all_type_supports.add( source_type_support)
        # self._all_type_supports.add( dest_type_support)

        serializer = Serializer(source_type_support,
                                base_type.__name__,
                                dest_type_support,
                                serializer_name=serializer_name,
                                additional_parameters=dest_type_support.serializer_additional_parameters())

        self._logger.info("Registering serializer {}( {} ...) -> {}".format(serializer.func_name(), source_type_support, dest_type_support))

        if serializer.func_name() in serializers:
            raise Exception("Looks like you define the same serializer twice : from {} to {} following a schema defined by {} (serializer function is '{}'(...)). Maybe you should use qualify the name further (using series name)".format( serializer.source_type_support, serializer.destination_type_support, serializer.base_type_name, serializer.func_name()))

        serializers[ serializer.func_name() ] = serializer

        fields, relations, single_rnames, knames, props = sqla_attribute_analysis(base_type)

        # --- INSTANCE MANAGEMENT ---------------------------------------------

        # serializer.append_code("print(\"*** DBG: {}\".format(str(source)[:500]))")

        # Caching

        serializer.append_blank()
        serializer.append_code("# Caching is more for reusing instances and prevent reference cycles than speed.")
        source_type_support.cache_key( serializer, "cache_key", "source",
                                       make_cache_base_name(source_type_support, dest_type_support),)
        serializer.append_code("if (cache_key is not None) and (cache_key in cache):")
        serializer.indent_right()
        serializer.append_code(    "# We have already transformed 'source'")
        serializer.append_code(    "return cache[cache_key]")
        serializer.indent_left()

        # Create destination instance

        serializer.append_blank()
        serializer.append_code("# Check if new instance has to be created")
        serializer.instance_mgmt( knames, source_type_support, dest_type_support)

        # --- FIELDS (key and non-key) ----------------------------------------

        fields_names = fields.keys()
        relations_names = relations # a map from "relation name" to "relation class"

        source_instance = "source"
        dest_instance = "dest"

        fields_to_copy = []
        fields_to_skip = []

        for field in sorted(list(fields_names)):
            if field in knames:
                # Keys will have a special treatment, see below.
                continue
            elif field in fields_control and fields_control[field] == SKIP:
                serializer.append_code("# Skipped field {}".format(field))
                continue
            else:
                fields_to_copy.append( field)

        # for f,fc in fields_control.items():
        #     if type(fc) == CodeWriter:
        #         dest_type_support._func_fields[field] = fc

        # Whatever the result of the cache, we'll have to serialize at least
        # the values  of the key fields.

        serializer.append_blank()
        serializer.append_code("# Copy key fields")
        self._field_copy( serializer, source_type_support, source_instance, dest_type_support, dest_instance, knames)



        serializer.append_blank()
        serializer.append_code("# Copy non-key fields")
        self._field_copy( serializer, source_type_support, source_instance, dest_type_support, dest_instance, fields_to_copy)



        # --- INSTANCE CHECK --------------------------------------------------

        # the place to decide what to do with the newly created instance
        # before connecting it to its relationships. For example,
        # you can merge the instance into SQLAlchemy's session here.

        serializer.append_blank()
        dest_type_support.check_instance_serializer( serializer, "dest")


        # When writing to a an instance I, we make sure the I
        # we write, will be a "short" one if I equals another instance
        # which was already serialized. By short, we mean we write
        # something that contains just enough information to
        # receonstruct the real thing while deserializing.
        # IOW, when we have the same object appearing several
        # times during the serialisation we make sure we serialize
        # it completely the first time and we serialize a shortcut
        # to it the other times.

        serializer.append_blank()
        serializer.append_code("# We update the cache before calling other serializers.")
        serializer.append_code("# This will protect us against circular references.")
        dest_type_support.cache_on_write( serializer,
                                          source_type_support, "source",
                                          make_cache_base_name(source_type_support, dest_type_support), "dest")


        # Some sanity check

        for name in fields_control: # Bug! this should look at relations only
            if (name not in relations) and (name not in single_rnames) and (name not in fields_names) and (name in fields_control and (name not in props) and type(fields_control[name]) != CodeWriter):
                self._logger.warn("The relation or field or property {}.{} you use in a field control doesn't exist. We know these : {}.".format( base_type.__name__, name, ','.join( list(relations.keys()) + list(single_rnames.keys()))))

        # --- PYTHON PROPERTIES ------------------------------------------------

        properties_to_copy = []

        for prop_name in props:
            if prop_name in fields_control:
                prop_model = fields_control[prop_name]

                if prop_model == SKIP:
                    serializer.append_code("# Skipped python property {} (has skip)".format( prop_name))

                elif prop_model == COPY:
                    properties_to_copy.append( prop_name)

                elif 'LIST' in prop_model:

                    # Properties as list

                    relation_serializer = prop_model['LIST']
                    rel_source_type_support = relation_serializer.source_type_support
                    rel_destination_type_support = relation_serializer.destination_type_support
                    serializer_code = relation_serializer.call_code( rel_destination_type_support.serializer_additional_parameters())

                    self._logger.debug("walker: in {} ({}), using a serializer for a property '{}', converting {} -> {}".format( base_type, source_type_support, prop_name, relation_serializer.source_type_support, relation_serializer.destination_type_support))

                    rel_destination_type_support.sequence_copy(
                        serializer, "source", "dest", prop_name,
                        source_type_support, dest_type_support,
                        rel_source_type_support,
                        serializer_code,
                        None,
                        make_cache_base_name(source_type_support, dest_type_support))

                else:
                    raise Exception("The property {}.{} is not well modelled in the field controls".format( base_type, prop_name))
            else:
                serializer.append_code("# Skipped python property {} (not in field control)".format( prop_name))

        if properties_to_copy:
            serializer.append_blank()
            serializer.append_code("# Properties copied without transformation")
            self._field_copy( serializer, source_type_support, source_instance, dest_type_support, dest_instance, properties_to_copy)

        # --- RELATIONS represented as single item ----------------------------

        for relation_name in single_rnames:

            if relation_name not in fields_control:
                raise Exception("Don't know how to serialize {}.{} because you didn't specify a field control for it.".format( base_type.__name__, relation_name))

            # if (relation_name not in fields_control) or fields_control[relation_name] != SKIP:
            if (relation_name in fields_control) and fields_control[relation_name] != SKIP:
                serializer.append_code('# Relation {} (single)'.format(relation_name))

                relation_serializer = fields_control[relation_name]

                assert isinstance( relation_serializer, Serializer), "Expected a relation serializer, got '{}'".format(relation_serializer)

                fk_name = next(iter(getattr( base_type, relation_name).property.local_columns)).name

                # This is tricky. The first part of the if ensures
                # there is a child to serialize. The presence of the
                # child is reprsented by the existence of an object.

                serializer.append_code( "if ({}):".format(
                    source_type_support.gen_is_single_relation_present("source", relation_name)))

                # serializer.append_code( "if ({} is not None) or ({} is None):".format(
                #     source_type_support.gen_is_single_relation_present("source", relation_name),
                #     dest_type_support.gen_read_field("dest", fk_name)))

                serializer.indent_right()

                serializer_code = relation_serializer.call_code(
                    relation_serializer.destination_type_support.serializer_additional_parameters())

                serializer.append_code(
                    dest_type_support.gen_write_field(
                        "dest", relation_name, serializer_code(
                            source_type_support.gen_read_field("source", relation_name),
                            None)))

                serializer.indent_left()

            else:
                serializer.append_code("# Skipped single relation '{}'".format(relation_name))

        # --- RELATIONS represented as sequence -------------------------------


        rel_to_walk = dict()

        for name, v in relations.items():

            if name in fields_control:
                if fields_control[name] == SKIP:
                    rel_to_walk[name] = SKIP
                elif isinstance( fields_control[name], Serializer):
                    rel_to_walk[name] = fields_control[name]
                elif type( fields_control[name]) == dict:
                    rel_to_walk[name] = fields_control[name].copy()
                    if "relation" not in rel_to_walk[name]:
                        rel_to_walk[name]["relation"] = v
                else:
                    raise Exception("The relation field '{}' for '{}' has a field control ({}) but I don't understand it.".format( name, base_type.__name__, fields_control[name]))
            else:
                raise Exception("Don't know how to serialize {}.{} because you didn't specify a field control for it.".format( base_type.__name__, name))



        for relation_name, relation in rel_to_walk.items():

            if relation == SKIP:
                serializer.append_code('# Skipped relation {}'.format(relation_name))
                serializer.append_blank()
                continue


            # The relation is expected to be denoted by the same name
            # in both the source and the destination, but that's just a
            # sane convention. So we give the possiblity to rename.

            assert isinstance(relation, Serializer)

            source_name = relation_name
            dest_name = relation_name
            rel_source_type_support = rel_destination_type_support = None
            relation_serializer = None

            relation_serializer = relation
            rel_source_type_support = relation_serializer.source_type_support
            rel_destination_type_support = relation_serializer.destination_type_support


            self._logger.debug("rel_source_type_support={}".format( rel_source_type_support))
            assert isinstance(relation_serializer, Serializer), "I want a Serializer for the relation '{}' of type {}".format(relation_name, relation_serializer)

            serializer_code = relation_serializer.call_code( rel_destination_type_support.serializer_additional_parameters())
            #serializer.append_blank()

            #dest_type_support = self.dest_factory.get_type_support( relations_names[relation_name])

            self._logger.debug("walker: In {} ({}), using a serializer for relation '{}', converting {} -> {}".format( base_type, source_type_support, relation_name, relation_serializer.source_type_support, relation_serializer.destination_type_support))

            rel_destination_type_support.relation_copy(
                serializer, "source", "dest", relation_name,
                source_type_support, dest_type_support,
                rel_source_type_support,
                serializer_code,
                relations[relation_name],
                make_cache_base_name(source_type_support, dest_type_support))

        dest_type_support.finish_serializer( serializer)

        serializer.append_code("return dest")
        serializer.indent_left()


        return serializer


def generated_code( serializers : list, additional_global_code : CodeWriter = CodeWriter()) -> str:
    """ Generate the code held in the serializers.
    Call this once you've got all your serializer ready.

    We generate code in a smart way (avoiding code duplication etc.)
    """

    # Using set avoids code duplication
    type_supports = set(  [ s.source_type_support      for s in serializers])
    type_supports.update( [ s.destination_type_support for s in serializers] )

    scode = [ "# Generated by Pyxfer on {}".format( datetime.now()) ]

    #scode.append("cache = dict()")

    global_code_fragments = [ set( ) ]
    global_code_fragments[0].add( additional_global_code.generated_code())

    # Group code fragments, deduplicates them and
    # preserve intra-group order.

    for ts in sorted( type_supports, key=lambda ts:ts.type_name()):
        fragments = ts.gen_global_code()

        if type(fragments) == list:
            if len(fragments) > len(global_code_fragments):
                delta = len(fragments) - len(global_code_fragments)
                global_code_fragments.extend( [ set() for i in range(delta) ] )

            for i in range( len( fragments)):
                global_code_fragments[i].add( fragments[i].generated_code())
        else:
            #print( ts)
            #print( f.generated_code())
            global_code_fragments[0].add( fragments.generated_code())

    for level in global_code_fragments:
        for frag in level:
            if frag: # clean empty fragments (should be useless, but I'm not alawys clean :-))
                scode.append(frag)

    for s in sorted( serializers, key=lambda s:s.func_name()):
        scode.append( s.generated_code())

    if scode:
        return "\n\n".join(scode)
    else:
        return ""



class SQLAAutoGen:
    """ This is a collection of methods that allows to set up
    the code generation more easily.

    It is tuned for SQLAlchemy objects.
    """

    def __init__(self, source_ts : TypeSupport,
                 dest_ts : TypeSupport,
                 logger : logging.Logger = _default_logger):
        """ The TypeSupport provided are expected to have
        parameter-less constructors.
        """

        self._logger = logger

        # We use TypeSupport factories to reuse instances of the TypeSupports
        # This allows the type supports to generate code more
        # efficiently (else they will generate the same code
        # often). This maps a TypeSupport class to a factory that
        # creates instance of that TypeSupport.
        self._ts_factories = dict()

        self.walker = SQLAWalker()

        # Where we'll store the serializers we'll produce.
        self._serializers = dict()

        self.set_type_supports( source_ts, dest_ts)

    def set_type_supports( self, source_ts, dest_ts):
        assert source_ts and dest_ts,"Missing type support"
        assert source_ts != dest_ts, "Serializing from one type to itself doesn't make sense"

        self._base_source_ts = source_ts
        self._base_dest_ts = dest_ts

        if self._base_source_ts not in self._ts_factories:
            self._ts_factories[self._base_source_ts] = TypeSupportFactory( source_ts, self._logger)

        if self._base_dest_ts not in self._ts_factories:
            self._ts_factories[self._base_dest_ts] = TypeSupportFactory( dest_ts, self._logger)

    def reverse(self):
        self._base_source_ts,self._base_dest_ts = self._base_dest_ts,self._base_source_ts

    def type_support(self, ts, base):
        return self._ts_factories[ts].get_type_support( base)

    def _make_serializer( self, type_, fields_control, serializer_name : str = None):

        source_type_support = self._ts_factories[self._base_source_ts].get_type_support( type_)
        dest_type_support = self._ts_factories[self._base_dest_ts].get_type_support( type_)

        s = self.walker.walk( source_type_support, type_,
                              dest_type_support, fields_control,
                              serializer_name)
        return s

    def make_serializers( self, models_fc, series_name = None):
        """Generate serializers for a collection of models.

        Returns a dict mapping SQLA mappers to their Serializer
        objects.

        models_fc : a dict containing SQLAclhemy mappers as keys
                    and "controls" that describe how to handle their
                    fields as value. Fields which are not described
                    in the controls are automatically serialized.

        """

        if series_name is not None:
            self._logger.debug( sorted( [ "{} series:{}".format(c[0].__name__, c[1]) for c in self._serializers.keys()]))

        serializers = dict()

        source_factory = self._ts_factories[self._base_source_ts]
        dest_factory = self._ts_factories[self._base_dest_ts]

        for base_type, fields_control in models_fc.items():
            source_type_support = source_factory.get_type_support( base_type)
            dest_type_support = dest_factory.get_type_support( base_type)

            # Create "place holders" (Serializers are not built on __init__)

            serializers[ (base_type, series_name) ] = Serializer( source_type_support, base_type.__name__, dest_type_support, series_name)

        # The following code makes sure the serializers are built in
        # the right order (remember serializers depend on each other
        # thus their definition order is important). This not trivial
        # and helps a lot while declaring fields controls (because
        # the user can declare them in any order).

        # At this point, models_fc contains all the SQLA models we
        # want to serialize.

        do_now = dict(models_fc)
        do_later = dict()

        # On each iteration, some (model, field controls) pairs
        # will be removed from do_now and, possibly, put in
        # do_later.
        while do_now or do_later:

            dbg_missing_deps = []
            serializers_made = False
            for base_type, fields_control in do_now.items():
                fc = dict(fields_control) # shallow copy !
                ftypes, rnames, single_rnames, knames, props = sqla_attribute_analysis( base_type)

                # Recurse down the base_type (SQLAlchemy model)

                # IMPORTANT : we don't look for SQLA models ourselves,
                # we leave it to the user to enumerate all the models
                # he wants us to analyze.

                has_unsatisfied_deps = False

                for prop_name in props:
                    if prop_name in fields_control:
                        prop_model = fields_control[prop_name]

                        self._logger.debug( prop_model)

                        if prop_model in ( SKIP, COPY):
                            fc[prop_name] = prop_model

                        elif 'LIST' in prop_model:

                            class_model = prop_model['LIST']

                            if not class_model:
                                raise Exception( "I expect a model class for property '{}' because it's described as LIST".format(prop_name))

                            # There are several aspects :
                            # * The fact it's a list of something
                            # * The fact this list may only be read by some TypeSupport
                            #raise Exception("zulu")
                            #pass
                            fc[prop_name] = { 'LIST' : serializers[ (class_model, series_name) ] }
                        else:
                            raise Exception("The property {}.{} is not well modelled in the field controls".format( base_type, prop_name))


                for relation_name in merge_dicts( rnames, single_rnames):

                    # Did the user request to skip this relation ?
                    if relation_name in fields_control and \
                       fields_control[relation_name] == SKIP:
                        continue

                    # analyze the relation
                    relation = getattr( base_type, relation_name)
                    relation_target = inspect(relation).mapper.class_

                    if relation_target not in models_fc:
                        raise Exception("{} For relation {}.{}, I don't know what to do with its target type : {}. You must skip that relation or tell me about its target type in the fields controls.".format( (series_name and "In series '{}',".format(series_name)) or "", base_type, relation_name, relation_target))

                    self._logger.debug("Analyzing, in series '{}' of '{}', relation '{}' of type '{}'".format(
                        series_name, base_type, relation_name, relation_target))

                    k = (relation_target, series_name)
                    k_alternate = (relation_target, None)

                    if k in serializers:
                        self._logger.debug("Found serializer : {}, named {}".format(k, serializers[ k].func_name()))
                        # We know how to handle that relation,
                        # so we store that in the fields controls
                        fc[relation_name] = serializers[ k]

                    elif k_alternate in self._serializers:
                        self._logger.debug("Found alternate serializer : {}".format(k_alternate))
                        fc[relation_name] = self._serializers[ k_alternate]

                    else:
                        msg = "{}.{} of type {}".format(
                            base_type.__name__, relation_name, relation_target.__name__)
                        dbg_missing_deps.append( msg)
                        has_unsatisfied_deps = True

                        self._logger.debug( serializers)
                        self._logger.debug( self._serializers)
                        self._logger.debug( k)
                        self._logger.debug( k_alternate)
                        self._logger.debug( msg)

                        #exit()
                        # I could break out of the loop, but I let it
                        # go so that the missing deps array is
                        # completely built, which in turn will improve
                        # error reporting.


                # At thuis point, we're done with analyzing the relationships
                # of the current SQLA model class.

                if not has_unsatisfied_deps:
                    # All the relationships of the the model have
                    # a corresponding serializer. Therefore we can
                    # build the serializer for this model.

                    serializers[ (base_type, series_name) ] = self._make_serializer( base_type, fc, series_name)
                    serializers_made = True
                else:
                    # Some relationships of the SQLA model have no
                    # corresponding serializer. This means that we'll
                    # have to build serializers for other models
                    # (hoping to find the missing one) So we now
                    # remind to reanalyse the SQLAModel later on.
                    do_later[base_type] = fields_control


            if not serializers_made:
                self._logger.debug( "serializers : {}".format( str( serializers)))
                self._logger.debug( "to do next  : {}".format( str( do_later)))
                self._logger.error( "missing deps: {}".format( dbg_missing_deps))

                msg = ""
                if series_name:
                    msg = " (while building series '{}')".format(series_name)
                raise Exception("Don't know what to do with these fields : {}{}. Maybe I don't know wabout these mappers ?".format( ", ".join( sorted( dbg_missing_deps)), msg))

            do_now = do_later
            do_later = dict()

        s = list( serializers.values())
        self._serializers.update( serializers)
        return s

    def serializer_by_name( self, klass, series_names : str):
        return self._serializers[ (klass, series_names) ]

    @property
    def serializers(self):
        return list( self._serializers.values())
