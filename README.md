# Slashem (SolrRogue)

Slashem (SolrRogue) is a type-safe internal Scala DSL for constructing and
executing find requests against SOLR. It is a rogue-like for solr. It is not
currently full expressive to SOLR's query functionality, but covers the main
use cases that we encountered.

## Building and Installation

Use sbt (simple-build-tool) to build:

    $ sbt clean update package

The finished jar will be in 'target/'.

## Examples
[QueryTest.scala] contains sample queries and shows the corresponding query.
[SolrRogueTest.scala] countains some sample records.

## Dependencies

lift, joda-time, junit, finagle, jackson. These dependencies are managed by 
the build system.

## Warnings

This is still a very early version. There are likely bugs (sorry!). Let us know
if you find any. While we can't promise timely fixes, it will help :)

## Maintainers

Slashem (SolrRogue) was initial developed by Foursquare Labs for internal use. 
The majourity of our solr related calls at Foursquare go through this library. 
The current maintainers are:

- Jon Shea jonshea@foursquare.com
- Govind Kabra govind@foursquare.com
- Holden Karau holden@foursquare.com

Contributions welcome!