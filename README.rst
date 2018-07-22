Pyxfer
==========

Come back later, this is still alpha code !

Pyxfer is a tool to *generate code* to serialize/marshal/transform
from one source type to a destination type.  We generate code because
it makes super fast serializers, and much less terse than runtime
attribute analysis (the generated code is quite readable). Right now
Pyxfer is wired to work with SQLAlchemy_, but nothing prevents it
to work on other structured data.

.. _SQLAlchemy: http://www.sqlalchemy.org/

How to serialize
----------------

Here's a small example::

        # First you describe which types will be
        # serialized (those are the "walked" ones)
        model_and_field_controls = { Order : {},
                                     OrderPart : { 'order' : SKIP } }

        # Factories to create the TypeSupport which in turn
        # will generate code fragments to read/write the
        # objects of the corresponding types. Here SQLATypeSupport
        # will handle SQLA entities and SQLADictTypeSupport will
        # handle dict's representation of SQLA entities. So,
        # in a word, we'll serialize SQLA entities to/from dicts.

        sqla_factory = TypeSupportFactory( SQLATypeSupport )
        dict_factory = TypeSupportFactory( SQLADictTypeSupport )
        walker = SQLAWalker()

        # Build serializers to serialize from SQLA objects to dicts
        cgq = CodeGenQuick( sqla_factory, dict_factory, walker)
        s1 = cgq.make_serializers( model_and_field_controls)

        # Build serializers to serialize in the reverse direction
        cgq = CodeGenQuick( dict_factory, sqla_factory,  walker)
        s2 = cgq.make_serializers( model_and_field_controls)

        # Generate the code and compile it
        gencode = generated_code( list(s1.values()) + list(s2.values()) )
        print(gencode)
        self.executed_code = dict()
        exec( compile( gencode, "<string>", "exec"), self.executed_code)



General architecture
--------------------

Pyxfer is a bit different than traditional serialization
frameworks because it is made to generate code instead of providing
"ready to use" functionality.

The difficult step is the first one : generate code. This requires a
bit of understanding of the architecture of Pyxfer.  There are two
important classes :

* The ``TypeSupport`` whose job is to provide code fragments that will
  read/write/update an object of a given type (say, a dict, a SQLA
  object).
* The ``Walker`` whose job is to guide the creation of full blown
  serializer according to "map" which is given by a model object (in
  its current state, Pyxfer is only able to "walk" SQLA mappers,
  but you could imagine it to walk an XSD or anything suitable).

For example, to generate a serializer that converts a representation
of an Order from a ``dict`` to an ``object``, you'll define a ``TypeSupport`` for
``dict`` and a ``TypeSupport`` for ``object``. Then, you'll define a ``Walker`` over
the ``object``, that will use both the ``TypeSupports`` (one to generate code
to read from the ``dict``, and the other to generate code to write to an
``object``).  Then, you'll execute the ``Walker`` which will produce the
serializers and, finally, use ``generate_code`` to, well, generate code :-)

A cool consequence of that is that you can transform a serializer in
to a deserializer just by exchanging both the ``TypeSupports``.

Pyxfer provides some tooling to allow you to create
``Walkers`` and ``TypeSupports`` easily in case you have many
classes to handle, see the TestCase_.

.. _TestCase :  https://github.com/wiz21b/pyxfer/blob/master/test.py
