Documentation
=============

My Test Framework is a Python library for doing the data manipulations. It works
with your favorite interface to provide idiomatic ways of adding,
subtracting, and multiplying the values given. 

Quick Start
===========

My Explanation content goes here 

Here's an HTML document I'll be using as an example throughout this
document. It's part of a story from `Alice in Wonderland`::

    html_doc = """
    <html><head><title>The Dormouse's story</title></head>
    <body>
    <p class="title"><b>The Dormouse's story</b></p>

    <p class="story">Once upon a time there were three little sisters; and their names were
    <a href="http://example.com/elsie" class="sister" id="link1">Elsie</a>,
    <a href="http://example.com/lacie" class="sister" id="link2">Lacie</a> and
    <a href="http://example.com/tillie" class="sister" id="link3">Tillie</a>;
    and they lived at the bottom of a well.</p>

    <p class="story">...</p>
    """

Does this look like what you need? If so, read on.

Installing Modules
==================

If you're using a recent version of Debian or Ubuntu Linux, you can
install with the system package manager:

    $ apt-get install python-module`

package name is published through PyPi, so if you can't install it
with the system packager, you can install it with ``easy_install`` or
``pip``. The package name is ``packagename``, and the same package
works on Python 2 and Python 3.

    $ easy_install packagename`

    $ pip install packagename`

    $ python setup.py install`

If all else fails, the license for package allows you to
package the entire library with your application. 

I use Python 2.7 and Python 3.2 to develop package, but it
should work with other recent versions.

