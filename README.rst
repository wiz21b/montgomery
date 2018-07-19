Montgomery
==========

Come back later, this is still alpha code !

Montgomery is a tool to *generate code* to serialize/marshal/transform
from one source type to a destination type.  We generate code because
it makes super fast serializers, much less terse than runtime
attribute analysis (the generated code is quite readable). Right now
Montgomery is wired to work with SQLAlchemy_, but nothing prevents it
to work on other structured data.

.. _SQLAlchemy: http://www.sqlalchemy.org/

How to serialize
----------------



General architecture
--------------------

Montgomery is a bit different than traditional serialization
frameworks because it is made to generate code instead of providing
"ready to use" functionality. So there are two steps : generate
serializer code, load code, serialize objects with the code.

The difficult step is the first one : generate code. This requires a
bit of understanding of the architecture of montgomery.  There are two
important classes :

* the ``TypeSupport`` whose job is to provide code fragments that will
  read/write/update an object of a given type (say, a dict, a SQLA
  objects)
* the ``Walker`` whose job is to guide the creation of full blown
  serializer according to "map" which is given by a model object (in
  its current state, Montgomery is only able to "walk" SQLA mappers,
  but you could imagine it to walk an XSD or anything suitable).

For example, to generate a serializer that converts a representation
of an Order from a dict to an object, you'll define a TypeSupport for
dict and a TypeSupport for object. Then, you'll define a walker over
the Object, that will use both the TypeSupports (one to generate code
to read from the dict, and the other to generate code to write to an
object).  Finally, you'll execute the walker which will produce the
code.

A cool consequence of that is that you can transform a serializer in
to a deserializer just by exchanging both the TypeSupports.

Montgomery provides some tooling to allow you to create
Walkers and TypeSupports easily in case you have many
classes to handle, see the TestCase_.

.. _TestCase :  https://github.com/wiz21b/montgomery/blob/master/test_montgomery.py

Montgomery is called like that because there was a great field
marshal (pun totally intended).

.. image:: Bernard_Law_Montgomery2.jpg
