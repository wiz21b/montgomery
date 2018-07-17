import unittest
from unittest import skip
from pprint import pprint

from montgomery.montgomery import SQLAWalker, SKIP, generated_code, TypeSupportFactory, CodeGenQuick
from montgomery.type_support import DictTypeSupport, SQLATypeSupport


from sqlalchemy import MetaData, Integer, ForeignKey, Date, Column, Float, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, backref, relationship


metadata = MetaData()
MapperBase = declarative_base(metadata=metadata)

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


engine = create_engine("sqlite:///:memory:")
MapperBase.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()




class Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        o = Order()
        o.hourly_cost = 1.23
        session.add(o)

        p = OrderPart()
        session.add(p)
        p.name = "Part One"
        p.order_id = o.order_id
        o.parts.append(p)

        p = OrderPart()
        p.name = "Part Two"
        session.add(p)
        o.parts.append(p)

        session.commit()

    @skip
    def test_happy(self):
        w = SQLAWalker()

        # Build the type supports for our mappers
        # A type support is a tool class that allows
        # to build the code fragments that will make
        # the serializer source code.
        order_ts = SQLATypeSupport( Order)
        order_part_ts = SQLATypeSupport( OrderPart)

        # We'll serialize to dict, so we use another
        # type support for that.
        dict_ts = DictTypeSupport()

        # Build the serializers. Note that in case of relationship,
        # they must be wired together with the "field_control"

        # Read the following line like this : using a walker, we
        # build a order_part_ser Serializer that will be able
        # to convert from source objects of a type represented by
        # order_part_ts TypeSupport to destination objects of a type represented
        # by dict_ts TypeSuppoer. The names of fields to read in the source
        # objects are given by the OrderPart mapper (the base type).
        # They're the same as those used to write in the destination
        # objects (provided an appropriate translation, i.e. here, in
        # dicts, fields names will become keys)

        # The SKIP avoids some infinite recursion
        order_part_ser = w.walk( order_part_ts, OrderPart, dict_ts,
                                 fields_control= { 'order' : SKIP})

        order_ser = w.walk( order_ts, Order, dict_ts,
                            fields_control= { 'parts' : order_part_ser})

        # Finally, we can generate the code
        gencode = generated_code( [order_part_ser, order_ser] )

        # It is very useful to read the generated code. We do
        # lots of efforts to make it clear (you can skip the caching
        # stuff first, because it's a bit trickier).
        print(gencode)

        # Oce you have the code, you can compile/exec it
        # like this or simply save it and import it when needed.
        self.executed_code = dict()
        exec( compile( gencode, "<string>", "exec"), self.executed_code)

        # Now we can serialize
        o = session.query(Order).first()
        assert len(o.parts) == 2

        # Calling is a bit awkward, you must read the generated
        # cod eto know the name of seriliaztion functions. Note
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

        order_part_unser = w.walk( dict_ts, OrderPart, order_part_ts,
                                   fields_control= { 'order' : SKIP})

        order_unser = w.walk( dict_ts, Order, order_ts,
                              fields_control= { 'parts' : order_part_unser})

        gencode = generated_code( [order_part_unser, order_unser] )
        print(gencode)
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


    def test_factories(self):

        cgq = CodeGenQuick( TypeSupportFactory( SQLATypeSupport ),
                            TypeSupportFactory( DictTypeSupport ),
                            SQLAWalker())

        # cgq.make_serializer( Order,
        #                      {'parts' : cgq.make_serializer( OrderPart, { 'order' : SKIP })})

        print("/////////////////////////////////////////")
        s = cgq.make_serializers( { Order : {},
                                    OrderPart : { 'order' : SKIP }
        } )
        print( s )

        gencode = generated_code( s.values() )
        print(gencode)
        self.executed_code = dict()
        exec( compile( gencode, "<string>", "exec"), self.executed_code)




if __name__ == "__main__":

    unittest.main()
