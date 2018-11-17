import io
import json
import unittest
import logging
from unittest import skip
from pprint import pprint, PrettyPrinter

from pyxfer.pyxfer import SQLAWalker, SKIP, generated_code
from pyxfer.sqla_autogen import SQLAAutoGen
from pyxfer.type_support import SQLADictTypeSupport, SQLATypeSupport, ObjectTypeSupport

logging.getLogger("pyxfer").setLevel(logging.DEBUG)

from sqlalchemy import MetaData, Integer, ForeignKey, Date, Column, Float, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, backref, relationship



metadata = MetaData()
MapperBase = declarative_base(metadata=metadata)

class Operation(MapperBase):
    __tablename__ = 'operations'

    operation_id = Column('operation_id',Integer,autoincrement=True,nullable=False,primary_key=True)

    name = Column('name',String,nullable=False)


class Order(MapperBase):
    __tablename__ = 'orders'

    order_id = Column('order_id',Integer,autoincrement=True,nullable=False,primary_key=True)

    start_date = Column('start_date',Date)
    cost = Column('hourly_cost',Float,nullable=False,default=0)

    parts = relationship('OrderPart', backref=backref('order'), cascade="delete, delete-orphan")


class OrderPart(MapperBase):
    __tablename__ = 'order_parts'

    order_part_id = Column('order_part_id',Integer,autoincrement=True,nullable=False,primary_key=True)

    order_id = Column('order_id',Integer,ForeignKey( Order.order_id),nullable=False)
    name = Column('name',String,nullable=False)

    operation_id = Column('operation_id',Integer,ForeignKey( Operation.operation_id),nullable=False)
    operation = relationship(Operation, uselist=False)




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


engine = create_engine("sqlite:///:memory:")
MapperBase.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def print_code( gencode : str):
    lines = gencode.split("\n")
    for i in range( 0, len( lines)):
        lines[i] = "{:5}: {}".format(i+1, lines[i])
    print( "\n".join( lines) )


def rename_ids( d, new_id):
    # Since ID's (in seriliazed dict's) are based on python id(...),
    # they differ from a python to another. That's no problem for
    # execution, but that'sone problem when writing expected. Here, we
    # tame those values.

    # FIXME : REUSE tags not handled !!!

    if type(d) == dict:
        if SQLADictTypeSupport.ID_TAG in d:
            id_value = d[SQLADictTypeSupport.ID_TAG]

            if id_value in new_id:
                d[SQLADictTypeSupport.ID_TAG] = new_id[id_value]
            else:
                new_id[id_value] = len(new_id)
                d[SQLADictTypeSupport.ID_TAG] = new_id[id_value]

        for key in sorted( d.keys()):
            rename_ids( d[key], new_id)

    elif type(d) == list:
        for entry in d:
            rename_ids( entry, new_id)

    return d

def canonize_dict( d : dict):
    rename_ids( d, dict())
    s = io.StringIO()
    PrettyPrinter(stream=s).pprint( d)
    return s.getvalue()

class Test(unittest.TestCase):

    def setUp(self):

        session.query(Order).delete()
        session.query(OrderPart).delete()
        session.query(Operation).delete()
        session.commit()


        # Just building up some object graph with SQLAlchemy

        self._op = Operation()
        self._op.operation_id = 12
        self._op.name = "lazer cutting"
        session.add( self._op)

        o = Order()
        o.order_id = 10000
        o.hourly_cost = 1.23
        session.add(o)

        p = OrderPart()
        session.add(p)
        p.name = "Part One"
        p.order_id = o.order_id
        o.parts.append(p)
        p.operation = self._op

        p = OrderPart()
        p.name = "Part Two"
        session.add(p)
        o.parts.append(p)
        p.operation = self._op

        session.commit()

    def _gen_code( self, source_ts, dest_ts):
        model_and_field_controls = find_sqla_mappers( MapperBase)

        # Avoid infinite recursion
        model_and_field_controls[OrderPart] = { 'order' : SKIP }

        # Serialization one way
        sqag1 = SQLAAutoGen( source_ts, dest_ts)
        sqag1.make_serializers( model_and_field_controls)

        # Serialization in the opposite direction
        sqag2 = SQLAAutoGen( dest_ts, source_ts)
        sqag2.make_serializers( model_and_field_controls)

        gencode = generated_code( sqag1.serializers + sqag2.serializers)
        self.executed_code = dict()
        print_code(gencode)
        exec( compile( gencode, "<string>", "exec"), self.executed_code)



    def test_load_and_append_element(self):
        self._gen_code(SQLATypeSupport, SQLADictTypeSupport)

        o = session.query(Order).filter(Order.order_id == 10000).one()


        serialized = self.executed_code['serialize_Order_Order_to_dict']( o, None, dict())
        pprint( serialized)

        serialized['parts'].append(
            { 'name': 'part 3',
              'operation_id': 12,
              'order_id': serialized['order_id'],
              'order_part_id': None} )

        with session.no_autoflush:
            self.executed_code['serialize_Order_dict_to_Order']( serialized, o, session, dict())

        session.commit()

        assert len(o.parts) == 3
        assert o.parts[1].name == "Part Two"
        assert o.parts[2].name == "part 3"

    def test_update_object( self):
        self._gen_code(SQLATypeSupport, ObjectTypeSupport)
        o = session.query(Order).filter(Order.order_id == 10000).one()
        s = self.executed_code['serialize_Order_Order_to_CopyOrder']( o, None, dict())
        session.commit()

        s.parts[0].name = "Changed"

        with session.no_autoflush:
            o = session.query(Order).filter(Order.order_id == 10000).one()
            s = self.executed_code['serialize_Order_CopyOrder_to_Order']( s, o, session, dict())
        session.commit()

        # update what needs to
        assert o.parts[0].name == "Changed"

        # not updated the rest
        assert len(o.parts) == 2
        assert o.parts[1].name == "Part Two"

    def test_load_object(self):
        self._gen_code(SQLATypeSupport, ObjectTypeSupport)
        o = session.query(Order).filter(Order.order_id == 10000).one()
        s = self.executed_code['serialize_Order_Order_to_CopyOrder']( o, None, dict())
        session.commit()

        assert len(s.parts) == 2
        assert s.parts[0].name == 'Part One'
        assert s.parts[1].name == 'Part Two'
        assert type(s) == self.executed_code['CopyOrder']
        assert type(s.parts[0]) == self.executed_code['CopyOrderPart']


        p = self.executed_code['CopyOrderPart']() # Create new CopyOrderPart object
        p.name = 'Part Three'
        p.order_id = s.order_id # Pyxfer doesn't find this alone :-(
        s.parts.append(p)

        op = self.executed_code['CopyOperation']()
        op.name = "zorglub"
        s.parts[-1].operation = op

        with session.no_autoflush:
            o = session.query(Order).filter(Order.order_id == 10000).one()
            s = self.executed_code['serialize_Order_CopyOrder_to_Order']( s, o, session, dict())
        session.commit()

        assert len(o.parts) == 3

    def test_simple_creation(self):
        model_and_field_controls = find_sqla_mappers( MapperBase)
        model_and_field_controls[OrderPart] = { 'order' : SKIP }
        sqag1 = SQLAAutoGen( SQLATypeSupport, SQLADictTypeSupport)
        sqag1.make_serializers( model_and_field_controls)

        sqag2 = SQLAAutoGen( SQLADictTypeSupport, SQLATypeSupport)
        sqag2.make_serializers( model_and_field_controls)

        gencode = generated_code( sqag1.serializers + sqag2.serializers )
        self.executed_code = dict()
        print_code(gencode)
        exec( compile( gencode, "<string>", "exec"), self.executed_code)

        o = Order()
        p1 = OrderPart()
        p1.name = "part 1"
        p1.operation = self._op
        p2 = OrderPart()
        p2.name = "part 2"
        p2.operation = self._op
        o.parts.append(p1 )
        o.parts.append(p2 )

        serialized = self.executed_code['serialize_Order_Order_to_dict']( o, None, dict())
        pprint( serialized)

        json.dumps(serialized)

        with session.no_autoflush:
            o = self.executed_code['serialize_Order_dict_to_Order']( serialized, None, session, dict())

        session.commit()

    # We skip that test because the situation it checks is super complicated
    # and, for the momement, not supported by pyxfer...
    @skip
    def test_two_new_instances_are_double(self):
        model_and_field_controls = find_sqla_mappers( MapperBase)
        model_and_field_controls[OrderPart] = { 'order' : SKIP }
        sqag1 = SQLAAutoGen( SQLATypeSupport, SQLADictTypeSupport)
        sqag1.make_serializers( model_and_field_controls)

        sqag2 = SQLAAutoGen( SQLADictTypeSupport, SQLATypeSupport)
        sqag2.make_serializers( model_and_field_controls)

        gencode = generated_code( sqag1.serializers + sqag2.serializers )
        self.executed_code = dict()
        print_code(gencode)
        exec( compile( gencode, "<string>", "exec"), self.executed_code)

        o = Order()
        p = OrderPart()
        # p appears twice in the data; so it will happen once in the dict
        # version and anotehr one as a shortcut. But since it's PK has not been set, we
        # must use some surrogate key :-)

        o.parts.append(p )
        o.parts.append( OrderPart())
        o.parts.append(p )

        serialized = self.executed_code['serialize_Order_Order_to_dict']( o, None, dict())
        pprint( serialized)


        with session.no_autoflush:
            o = self.executed_code['serialize_Order_dict_to_Order']( serialized, None, session, dict())

        assert o.order_id == None
        assert len(o.parts) == 3, "len is {}".format(len(o.parts))
        assert o.parts[0] != o.parts[1]
        assert o.parts[2] != o.parts[1]
        assert o.parts[0] == o.parts[2]
        assert o.parts[0].order_part_id == None
        assert o.parts[1].order_part_id == None
        assert o.parts[2].order_part_id == None

        session.rollback()

    #@skip
    def test_happy(self):
        # This is the walker, one of the basic buidling blocks of Pyxfer.
        # The walker's job is to go around a model of the objects
        # to serializer to dtermine the fields and the relationships
        # to work on. With that information, we'll be able to build
        # serializers.

        w = SQLAWalker()

        # Build the type supports for our mappers
        # A type support is a tool class that allows
        # to build the code fragments that will make
        # the serializer source code.
        # The TypeSupport class will be used by
        # the walker to actually generate code.

        order_ts = SQLATypeSupport( Order)
        order_part_ts = SQLATypeSupport( OrderPart)

        # We'll serialize to dict, so we use another
        # type support for that (SQLADict are a bit
        # more clever than regular dicts when it comes
        # to serialization).

        order_dts = SQLADictTypeSupport( Order)
        order_part_dts = SQLADictTypeSupport( OrderPart)

        # Build the serializers. Note that in case of relationship,
        # they must be wired together with the "field_control"

        # Read the following line like this : using a walker, we build
        # a order_part_ser Serializer that will be able to convert
        # from source OrderPart to destination objects dict's.  The
        # names of fields to read in the source objects are given by
        # the OrderPart mapper (the base type). These names will be
        # used both when reading and writing from source to destination
        # objects. Which fields will be seriliaized or not is indicated
        # by the fields_control (SKIP means don't serialize)

        # FIXME What about those fields WE WANT ?
        order_part_ser = w.walk( order_part_ts, OrderPart, order_part_dts,
                                 fields_control= { 'order' : SKIP, 'operation' : SKIP})

        order_ser = w.walk( order_ts, Order, order_dts,
                            fields_control= { 'parts' : order_part_ser})

        # Finally, we can generate the code
        gencode = generated_code( [order_part_ser, order_ser] )

        # It is very useful to read the generated code. We do
        # lots of efforts to make it clear (you can skip the caching
        # stuff first, because it's a bit trickier).
        print_code(gencode)

        # Once you have the code, you can compile/exec it
        # like this or simply save it and import it when needed.
        self.executed_code = dict()
        exec( compile( gencode, "<string>", "exec"), self.executed_code)

        # Now we can serialize
        o = session.query(Order).first()
        assert len(o.parts) == 2

        # Calling is a bit awkward, you must read the generated
        # code to know the name of seriliaztion functions. Note
        # that the names of thosee function follow a regular
        # pattern : walked_type_source type support_destination_type
        # support. If you had imported the code, then you're dev
        # environement would auto complete, which is much easier.

        serialized = self.executed_code['serialize_Order_Order_to_dict']( o, None, {})
        pprint( serialized)

        session.commit()

        # Once you got it, you can revert the serialization easily
        # Note that you if you use factories, this code can
        # be shared quite a lot.

        order_part_unser = w.walk( order_part_dts, OrderPart, order_part_ts,
                                   fields_control= { 'order' : SKIP, 'operation' : SKIP})

        order_unser = w.walk( order_dts, Order, order_ts,
                              fields_control= { 'parts' : order_part_unser})

        gencode = generated_code( [order_part_unser, order_unser] )
        print_code(gencode)
        self.executed_code = dict()
        exec( compile( gencode, "<string>", "exec"), self.executed_code)

        # Note that when one serializes to SQLAlchemy objects, one
        # needs a SQLA session (because we'll need to add new objects
        # to it).


        unserialized = self.executed_code['serialize_Order_dict_to_Order']( serialized, None, session, dict())
        pprint( unserialized)

        # Note 2 : the serializer we propose is smart enough to reload
        # objects alreay existing in the database. So you can use it
        # update objects rather than to create them.
        assert unserialized.order_id == o.order_id


    def test_autogen(self):

        # First you describe which types will be serialized.  Note
        # that the description of the type itself (fields,
        # relationships) is in fact provided by the SQLAlchemy
        # mappers.  By default, every field in a model will be
        # serialized (you have less control, but less to write too).
        # You can ask to skip some fields/relationships in order to
        # avoid unwanted recursion. If you need more control, check
        # the other test cases.

        # Note that the autogen functions won't recurse through your
        # mappers automatically.

        model_and_field_controls = find_sqla_mappers( MapperBase)

        # Let's specify things a bit more. In this case we don't
        # want order to be serialized as a part of an OrderPart
        # (this will avoid som unwated recursion)
        model_and_field_controls[OrderPart] = { 'order' : SKIP }

        # Build serializers to serialize from SQLA objects to dicts
        # The SQLAAutoGen class is just a big shortcut
        # to code generation.
        sqag1 = SQLAAutoGen( SQLATypeSupport, SQLADictTypeSupport)
        sqag1.make_serializers( model_and_field_controls)

        # Build serializers to serialize in the reverse direction
        # Note that we use the very same construction as above,
        # with parameters in a different order.

        sqag2 = SQLAAutoGen( SQLADictTypeSupport, SQLATypeSupport)
        sqag2.make_serializers( model_and_field_controls)

        # Generate the code of the serializers and compile it.
        # notice we gather all the seriliazers to generate the code
        # this will help the code generator to trim redundant code.

        gencode = generated_code( sqag1.serializers + sqag2.serializers )
        print_code(gencode)
        self.executed_code = dict()
        exec( compile( gencode, "<string>", "exec"), self.executed_code)

        # And of course, let's test it !

        o = session.query(Order).first()
        serialized = self.executed_code['serialize_Order_Order_to_dict']( o, None, dict())


        # This is the expected result. Note the optimisation we do for
        # the "operation" value. The first time it appears, we we give
        # its full value (a normal recursion). But when it appears a
        # second time, we limit ourselves to a key that identifies the
        # previous full value. This way, we won't replicate a dict
        # that appears several times in the serialisation.  That's
        # useful when many objects refer to a few other ones.  In our
        # case, many order parts were refering to a small set of well
        # defined operations. Of course, deserialization has to be
        # smart enough to understand that kind of shortcut (hint : it
        # is :-))

        expected = {'cost': 0.0,
                    'order_id': 10000,
                    'parts': [{'name': 'Part One',
                               'operation': {'name': 'lazer cutting', 'operation_id': 12},
                               'operation_id': 12,
                               'order_id': 10000,
                               'order_part_id': 1},
                              {'name': 'Part Two',
                               'operation': { '__PYXPTR': (12,) },
                               'operation_id': 12,
                               'order_id': 10000,
                               'order_part_id': 2}],
                    'start_date': None}

        expected_cano = canonize_dict( expected)
        s = canonize_dict( serialized)

        assert expected_cano == s, "Got\n {}; expected\n {}".format(s, expected_cano)

if __name__ == "__main__":

    unittest.main()
