import io
import unittest
import logging
from unittest import skip
from pprint import pprint, PrettyPrinter

from pyxfer.pyxfer import SQLAWalker, SKIP, generated_code, SQLAAutoGen, analyze_mappers
from pyxfer.type_support import SQLADictTypeSupport, SQLATypeSupport

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

    parts = relationship('OrderPart', backref=backref('order'))


class OrderPart(MapperBase):
    __tablename__ = 'order_parts'

    order_part_id = Column('order_part_id',Integer,autoincrement=True,nullable=False,primary_key=True)

    order_id = Column('order_id',Integer,ForeignKey( Order.order_id),nullable=False)
    name = Column('name',String,nullable=False)

    operation_id = Column('operation_id',Integer,ForeignKey( Operation.operation_id),nullable=False)
    operation = relationship(Operation, uselist=False)





engine = create_engine("sqlite:///:memory:")
MapperBase.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def print_code( gencode : str):
    lines = gencode.split("\n")
    for i in range( 1, len( lines)):
        lines[i] = "{:5}: {}".format(i, lines[i])
    print( "\n".join( lines) )


def rename_ids( d, new_id):

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

    @classmethod
    def setUpClass(cls):

        # Just building up some object graph with SQLAlchemy

        op = Operation()
        op.operation_id = 12
        op.name = "lazer cutting"
        session.add(op)

        o = Order()
        o.hourly_cost = 1.23
        session.add(o)

        p = OrderPart()
        session.add(p)
        p.name = "Part One"
        p.order_id = o.order_id
        o.parts.append(p)
        p.operation = op

        p = OrderPart()
        p.name = "Part Two"
        session.add(p)
        o.parts.append(p)
        p.operation = op

        session.commit()

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

        serialized = self.executed_code['serialize_Order_Order_to_dict']( o, None)
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


        unserialized = self.executed_code['serialize_Order_dict_to_Order']( serialized, None, session)
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

        model_and_field_controls = analyze_mappers( MapperBase)

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
        serialized = self.executed_code['serialize_Order_Order_to_dict']( o, None)


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
                    'order_id': 1,
                    'parts': [{'name': 'Part One',
                               'operation': {'name': 'lazer cutting', 'operation_id': 12},
                               'operation_id': 12,
                               'order_id': 1,
                               'order_part_id': 1},
                              {'name': 'Part Two',
                               'operation': {'operation_id': 12},
                               'operation_id': 12,
                               'order_id': 1,
                               'order_part_id': 2}],
                    'start_date': None}

        r = canonize_dict( expected)
        s = canonize_dict( serialized)

        print(r)
        print("-"*80)
        print(s)

        assert r == s

if __name__ == "__main__":

    unittest.main()
