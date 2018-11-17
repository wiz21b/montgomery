import logging

from sqlalchemy import inspect

from pyxfer.pyxfer import SQLAWalker, Serializer, SKIP, COPY
from pyxfer.type_support import TypeSupport, TypeSupportFactory
from pyxfer.utils import _default_logger, sqla_attribute_analysis, merge_dicts


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