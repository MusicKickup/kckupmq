# Wrapper library with common interface for different Message Queue implementations [![Build Status](https://secure.travis-ci.org/nemein/kckupmq.png?branch=master)](http://travis-ci.org/nemein/kckupmq)

Built for [Music Kickstarter](http://musickickstarter.com/), the first cloud-based record label.

This project is under development, but Redis backend is already in use in production servers.

## TODO:

* Write documentation

## Currently supported backends

* Redis (Persistent)
* RabbitMQ (currently PubSub only, no persistence on library end) (requires "npm install rabbit.js")
