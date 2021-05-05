GraphAware Neo4j TimeTree - RETIRED
==========================
                     
## GraphAware Neo4j TimeTree Has Been Retired
As of May 2021, this [repository has been retired](https://graphaware.com/framework/2021/05/06/from-graphaware-framework-to-graphaware-hume.html).

GraphAware TimeTree is a simple library for representing time in Neo4j as a tree of time instants. The tree is built on-demand,
supports resolutions of one year down to one millisecond and has time zone support. It also supports attaching event nodes to time instants (created on demand).

## Community vs Enterprise

This open-source (GPL) version of the module is compatible with GraphAware Framework Community (GPL), which in turn 
is compatible with Neo4j Community Edition (GPL) only. It *will not work* with Neo4j Enterprise Edition, which is a 
proprietary and commercial software product of Neo4j, Inc..

GraphAware offers an Enterprise version of the GraphAware Framework to licensed users of Neo4j Enterprise Edition.
Please [get in touch](mailto:info@graphaware.com) to receive access.

Getting the Software
--------------------

### Server Mode

When using Neo4j in the <a href="http://docs.neo4j.org/chunked/stable/server-installation.html" target="_blank">standalone server</a> mode, you will need the <a href="https://github.com/graphaware/neo4j-framework" target="_blank">GraphAware Neo4j Framework</a> and GraphAware Neo4j TimeTree .jar files (both of which you can <a href="http://graphaware.com/downloads/" target="_blank">download here</a>) dropped into the `plugins` directory of your Neo4j installation. Starting in version 3.2, Neo4j has increased security for procedures and functions (sandboxing). Procedures that use internal APIs have to be allowed in `$NEO4J_HOME/conf/neoj4.conf`. As an example: `dbms.security.procedures.unrestricted=ga.timetree.*`

After adding the required plugins and updating `neo4j.conf`, restart Neo4j and you will be able to use the REST APIs of the TimeTree. Also note the additional, optional, installation instructions in the [Automatic Event Attachment](https://github.com/graphaware/neo4j-timetree#automatic-event-attachment) section.

### Embedded Mode / Java Development

Java developers that use Neo4j in <a href="http://docs.neo4j.org/chunked/stable/tutorials-java-embedded.html" target="_blank">embedded mode</a>
and those developing Neo4j <a href="http://docs.neo4j.org/chunked/stable/server-plugins.html" target="_blank">server plugins</a>,
<a href="http://docs.neo4j.org/chunked/stable/server-unmanaged-extensions.html" target="_blank">unmanaged extensions</a>,
GraphAware Runtime Modules, or Spring MVC Controllers can include use the TimeTree as a dependency for their Java project.

#### Releases

Releases are synced to <a href="http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22timetree%22" target="_blank">Maven Central repository</a>. When using Maven for dependency management, include the following dependency in your pom.xml and edit the version number.

    <dependencies>
        ...
        <dependency>
            <groupId>com.graphaware.neo4j</groupId>
            <artifactId>timetree</artifactId>
            <version>A.B.C.D.E</version>
        </dependency>
        ...
    </dependencies>

#### Snapshots

To use the latest development version, just clone this repository, run `mvn clean install` and change the version in the
dependency above to A.B.C.D.E-SNAPSHOT.

#### Note on Versioning Scheme

The version number has two parts. The first four numbers indicate compatibility with Neo4j GraphAware Framework.
 The last number is the version of the TimeTree library. For example, version 2.0.3.4.3 is version 3 of the TimeTree
 compatible with GraphAware Neo4j Framework 2.0.3.4.

Using GraphAware TimeTree
-------------------------

TimeTree allows you to represent events as nodes and link them to nodes representing instants of time in order to capture
the time of the event's occurrence. For instance, if you wanted to express the fact that an email was sent on a specific
day, you would create a node labelled `Email` and link it to a node labelled `Day` using a `SENT_ON` relationship.

![email linked to day](https://github.com/graphaware/neo4j-timetree/raw/master/docs/image.jpg)

In order to be able to ask interesting queries, such as "show me all emails sent in a specific month", people <a href="http://neo4j.com/blog/modeling-a-multilevel-index-in-neoj4/" target="_blank">often built</a>
a time-tree in Neo4j with a root, years on the first level, months on the second level, etc. Something like this:

![time tree](https://github.com/graphaware/neo4j-timetree/raw/master/docs/image2.jpg)

One way of building such tree is of course pre-generating it, for instance using a Cypher query. The approach taken by GraphAware TimeTree is to build the tree on-demand, as nodes representing time instants are requested.
For example, you can ask the library "give me a node representing 24th May 2014". You'll get the node (Java) or its ID (REST) and can start linking to it.
In the background, a node representing May (labelled `Month`) and a node representing 2014 (labelled `Year`) will be created,
if they do not exist. Links between nodes on the same level as well as between levels are automatically maintained.

There are 4 types of relationships (hopefully self-explanatory):
* `CHILD`
* `NEXT`
* `FIRST`
* `LAST`

When using `SingleTimeTree`, the root of the tree is labelled `TimeTreeRoot`. You can create multiple time trees in your
graph, in which case you should use `CustomRootTimeTree` and supply a node from your graph that will serve as the root of the tree.

The graph above, if generated by GraphAware `SingleTimeTree`, would thus look like this:

![GraphAware TimeTree generated time tree](https://github.com/graphaware/neo4j-timetree/raw/master/docs/image3.jpg)

You can select the "resolution" of the time instant you will get from TimeTree. For instance, you only know that a certain
event happened in April, nothing more. In that case, you can request the node representing April. Equally, you can request
nodes representing concrete millisecond instants, if you so desire.

You may also provide a time-zone to the TimeTree APIs in order to create correctly labelled nodes for specific time instants.

Finally, the GraphAware TimeTree supports both attaching event nodes to time instants, and fetching events attached
to a time instant and all its children or between two time instants and all their children. For instance, you can ask
for all events that happened in April, which will return events attached to the April node as well as all its children
and their children, etc.

### Cypher

The TimeTree plugin offers a series of stored procedures in order to work with instant nodes and events efficiently.

#### Instant nodes

To get a node representing a time instant :

```
CALL ga.timetree.single({time: 1463659567468})
```

If you would like that the instant node is created if it doesn't exist for the given time, you can user single with option create instead :

```
CALL ga.timetree.single({time: 1463659567468, create: true})
```

This will create the time tree nodes in the graph :

![GraphAware TimeTree procedure merge](https://github.com/graphaware/neo4j-timetree/raw/master/docs/procedure1.png)

The following parameters can be passed in the map:

* `time`: (mandatory) the long representation of the time
* `resolution` : default resolution is `Day`
* `timezone` : default timezone is `UTC`
* `root`: by default the time instants are attached to the default TimeTreeRoot, you can pass a node that will be used as time tree root
* `create`: create the time tree node if not exist, by default to `false`

You can also get or create a node that represent the current time with the `now()` procedure :

```
CALL ga.timetree.now({})
```

Except for the `time` parameter, all other parameters can be passed in the map

You can also retrieve a range of instant nodes, by invoking the `range` procedure call :

```
CALL ga.timetree.range({start: 1463659567468, end: 1463859569504, create: true})
```

For the range call, an additional `create` parameter is available (by default to false). If set to true, the instant nodes will be created if they don't exist.

![GraphAware TimeTree range](https://github.com/graphaware/neo4j-timetree/raw/master/docs/procedure2.png)

#### Attaching events to the time tree

You can attach any node to a time instant with the `events.attach` procedure call :

```
CREATE (e:Email {text: "I used the timetree"})
WITH e
CALL ga.timetree.events.attach({node: e, time: 1463659567468, relationshipType: "SENT_ON"})
YIELD node RETURN node
```

![GraphAware TimeTree procedure attach](https://github.com/graphaware/neo4j-timetree/raw/master/docs/procedure3.png)

The following parameters can be passed in the map :

* `time`: (mandatory) the long representation of the time
* `node`: (mandatory) the event node to be attached to the time tree
* `relationshipType`: (mandatory) the name of the relationship type to be created between the event node and the instant node
* `direction`: default to `OUTGOING`, can be of `OUTGOING` or `INCOMING`
* `resolution` : default resolution is `Day`
* `timezone` : default timezone is `UTC`
* `root`: The time tree root to be used (default to the default TimeTreeRoot)

#### Retrieving events from the time tree

The most usage of the timetree is retrieving events from it. For finding events that occured at a specific time, you can use the `events.single` procedure call :

```
CALL ga.timetree.events.single({time: 1463659567468}) YIELD node RETURN node
```

![GraphAware TimeTree procedure events retrieve](https://github.com/graphaware/neo4j-timetree/raw/master/docs/procedure4.png)

The procedure yield more than the node. The relationship type as well as the direction between the event and the time instant can also be returned :

```
CALL ga.timetree.events.single({time: 1463659567468}) YIELD node, relationshipType, direction RETURN *
```

![GraphAware TimeTree procedure single and yield all](https://github.com/graphaware/neo4j-timetree/raw/master/docs/procedure5.png)

The following parameters can be passed in the map :

* `time`: (mandatory) the long representation of the time
* `relationshipTypes`: a List of relationshipType names that for constraining the search
* `direction`: default to `OUTGOING`, can be of `OUTGOING` or `INCOMING`
* `resolution` : default resolution is `Day`
* `timezone` : default timezone is `UTC`
* `root`: The time tree root to be used (default to the default TimeTreeRoot)

As for the time instants, you can also retrieve events for a range in time :

```
CALL ga.timetree.events.range({start: 1463659567468, end: 1463859569504}) YIELD node, relationshipType, direction RETURN *
```

![GraphAware TimeTree procedure retrieve in range](https://github.com/graphaware/neo4j-timetree/raw/master/docs/procedure6.png)

The same parameters from the `events.single` call apply for the `range` call, except `time` of course.

### REST API

When deployed in server mode, there are the following URLs that you can issue `GET` requests to:

* `http://your-server-address:7474/graphaware/timetree/single/{time}` to get a node representing a time instant, where time must be replaced by a `long` number representing the number of milliseconds since 1/1/1970. The default resolution is Day and the default time zone is UTC
* `http://your-server-address:7474/graphaware/timetree/single/{time}/events` to get events attached to a time instant, where time must be replaced by a `long` number representing the number of milliseconds since 1/1/1970. The default resolution is Day and the default time zone is UTC
* `http://your-server-address:7474/graphaware/timetree/range/{startTime}/{endTime}` to get nodes representing time instants between {startTime} and {endTime} (inclusive). The default resolution is Day and the default time zone is UTC
* `http://your-server-address:7474/graphaware/timetree/range/{startTime}/{endTime}/events` to get events that occurred between {startTime} and {endTime} (inclusive). The default resolution is Day and the default time zone is UTC
* `http://your-server-address:7474/graphaware/timetree/now` to get a node representing now. Defaults are the same as above.
* `http://your-server-address:7474/graphaware/timetree/{rootNodeId}/single/{time}` to get a node representing a time instant, where {time} must be replaced by a `long` number representing the number of milliseconds since 1/1/1970 and {rootNodeId} must be replaced by the ID of an existing node that should serve as the tree root. Defaults are the same as above.
* `http://your-server-address:7474/graphaware/timetree/{rootNodeId}/single/{time}/events` to get events attached to a time instant, where {time} must be replaced by a `long` number representing the number of milliseconds since 1/1/1970 and {rootNodeId} must be replaced by the ID of an existing node that should serve as the tree root. Defaults are the same as above.
* `http://your-server-address:7474/graphaware/timetree/{rootNodeId}/range/{startTime}/{endTime}/events` to get events that occurred between {startTime} and {endTime} (inclusive) and {rootNodeId} must be replaced by the ID of an existing node that should serve as the tree root. Defaults are the same as above.
* `http://your-server-address:7474/graphaware/timetree/{rootNodeId}/now` to get a node representing now, where {rootNodeId} must be replaced by the ID of an existing node that should serve as the tree root. Defaults are the same as above.

Additionally, you can issue `POST` requests to:

* `http://your-server-address:7474/graphaware/timetree/single/{time}` to get or create a node representing a time instant, where time must be replaced by a `long` number representing the number of milliseconds since 1/1/1970. The default resolution is Day and the default time zone is UTC
* `http://your-server-address:7474/graphaware/timetree/range/{startTime}/{endTime}` to get or create nodes representing time instants between {startTime} and {endTime} (inclusive). The default resolution is Day and the default time zone is UTC
* `http://your-server-address:7474/graphaware/timetree/now` to get or create  a node representing now. Defaults are the same as above.
* `http://your-server-address:7474/graphaware/timetree/{rootNodeId}/single/{time}` to get or create  a node representing a time instant, where {time} must be replaced by a `long` number representing the number of milliseconds since 1/1/1970 and {rootNodeId} must be replaced by the ID of an existing node that should serve as the tree root. Defaults are the same as above.
* `http://your-server-address:7474/graphaware/timetree/{rootNodeId}/now` to get or create a node representing now, where {rootNodeId} must be replaced by the ID of an existing node that should serve as the tree root. Defaults are the same as above.

You have four query parameters:

* `resolution`, which can take on the following values:
    * `Year`
    * `Month`
    * `Day`
    * `Hour`
    * `Minute`
    * `Second`
    * `Millisecond`
* `timezone`, which can be a String representation of any `java.util.TimeZone`
* `relationshipTypes`, which is a String representation of the `RelationshipType`s, one of which relate the event to a time instant, separated by a comma.
The default is all relationships, which is useful if you have different kinds of events occurring at the same time instant,
  and related to the time instant with different relationship types. Here the default will give you all events that occurred at that time instant.

  For instance, issuing the following request, asking for the hour node representing 5th April 2014 1pm (UTC time) in the
  GMT+1 time zone

      GET http://your-server-address:7474/graphaware/timetree/single/1396706182123?resolution=Hour&timezone=GMT%2B1

  on an empty database will result in the following graph being generated. The response body will contain the Neo4j node
  of the node representing the hour. You can then use it in order to link to it. Example response:
```json
{
  "id": 4,
  "properties": {
    "value": 14
  },
  "labels": [
    "Hour"
  ]
}
```
* `direction`, which is a String representation of the `Direction`, with which the event is related to the time instant from the time instant's point of view. Defaults to `INCOMING`. Permitted values are `INCOMING`,`OUTGOING`,`BOTH`.

![GraphAware TimeTree generated time tree](https://github.com/graphaware/neo4j-timetree/raw/master/docs/image4.jpg)

The response to calls returning events contain a list of events with relationship names attaching these to instants, e.g.:

```json
[
  {
    "node": {
      "id": 99,
      "properties": {
        "name": "eventA"
      },
      "labels": ["Event"]
    },
    "relationshipType": "STARTED_ON_DAY",
    "direction": "INCOMING"
  },
  {
    "node": {
      "id": 100,
      "properties": {
        "name": "eventB"
      },
      "labels": ["Event"]
    },
    "relationshipType": "ENDED_ON_DAY",
    "direction": "INCOMING"
  }
]
```

Attaching an event to a time instant requires a POST request to:

* `http://your-server-address:7474/graphaware/timetree/single/event` to attach an existing event node to a node representing a time instant.
* `http://your-server-address:7474/graphaware/timetree/{rootNodeId}/single/event` to attach an existing event node to a node representing a time instant, where {rootNodeId} must be replaced by the ID of an existing node that should serve as the tree root

The POST body resembles:

```json
{
  "node": {
    "id": 99
  },
  "relationshipType": "HAS_EVENT",
  "direction":"INCOMING",
  "timezone": "UTC",
  "resolution": "DAY",
  "time": 1403506278000
}
```

where

* `node.id` is the node ID of an existing event node
* `relationshipType` is the name of the relationship type that should be used to attach the event to the time instant.
* `direction` (optional) is the name of the direction (`INCOMING` or `OUTGOING`) that should be used to attach the event to the time instant, *from the tree's point of view*. By default, it is `INCOMING`, i.e., the relationship is directed from the event to the time instant.
* `timezone` is a String representation of any `java.util.TimeZone` (optional)
* `resolution` as described above (optional)
* `time` is a number representing the number of milliseconds since 1/1/1970

It is also possible to attach a brand new event that does not exist yet. In this case, specify `node.labels` and `node.properties`
instead if `node.id`. the body of the POST should resemble:

```json
{
  "node": {
    "properties": {
      "name": "eventA"
    },
    "labels": ["Event"]
  },
  "relationshipType": "HAS_EVENT",
  "direction": "INCOMING",
  "timezone": "UTC",
  "resolution": "DAY",
  "time": 1403506278000
}
```

### Automatic Event Attachment

All TimeTree versions compatible with Neo4j 2.2.0+ have the capability of automatically attaching events to the tree.

By default, any node created with label `Event` that contains a property named `timestamp` (with value as a `long` representing the timestamp in milliseconds),
will be attached to the tree with an outgoing relationship `AT_TIME` from the event node to tree.

Required configuration as well as overriding the defaults are specified in `neo4j.conf` as follows:

```
#For the framework to work at all, you need this
dbms.unmanaged_extension_classes=com.graphaware.server=/graphaware

# Runtime must be enabled like this
com.graphaware.runtime.enabled=true

# A Runtime module that takes care of attaching the events like this (TT is the ID of the module)
com.graphaware.module.TT.1=com.graphaware.module.timetree.module.TimeTreeModuleBootstrapper

# autoAttach must be set to true
com.graphaware.module.TT.autoAttach=true

# Optionally, nodes which represent events and should be attached automatically have to be defined (defaults to nodes with label Event)
com.graphaware.module.TT.event=hasLabel('Email')

# Optionally, a property on the event nodes that represents the the time (long) at which the event took place must be specified (defaults to "timestamp")
com.graphaware.module.TT.timestamp=time

# Optionally, a property on the event nodes that represents the node ID (long) of the root node for the tree, to which the event should be attached (defaults to "timeTreeRootId")
com.graphaware.module.TT.customTimeTreeRootProperty=rootId

# Optionally, a resolution can be specified (defaults to DAY)
com.graphaware.module.TT.resolution=HOUR

# Optionally, a time zone can be specified (defaults to UTC)
com.graphaware.module.TT.timezone=GMT+1

# Optionally, a relationship type with which the events will be attached to the tree can be specified (defaults to AT_TIME)
com.graphaware.module.TT.relationship=SENT_ON

# Optionally, a relationship direction (from the tree's point of view), with which the events will be attached to the tree can be specified (defaults to INCOMING)
com.graphaware.module.TT.direction=INCOMING

```

For more information on the `com.graphaware.module.TT.event` setting, i.e. how to write expressions that define which
nodes should be attached to the tree, please refer to [Inclusion Policies](https://github.com/graphaware/neo4j-framework/tree/master/common#inclusion-policies).

Examples of relevant expressions that can be used:

* `hasProperty('propertyName')` - returns boolean. Example: `hasProperty('name')`
* `getProperty('propertyName','defaultValue')` - returns Object. Example: `getProperty('name','unknown') == 'Michal'`
* `getDegree()` or `degree` - returns int. Examples: `degree > 1`
* `getDegree('typeOrDirection')` - returns int. Examples: `getDegree('OUTGOING') == 0` or `getDegree('FRIEND_OF') > 1000`
* `getDegree('type', 'direction')` - returns int. Examples: `getDegree('FRIEND_OF','OUTGOING') > 0`
* `hasLabel('label')` - returns boolean. Example: `hasLabel('Person')`

Of course, the expressions can be combined with logical operators, for instance:
* `hasLabel('Event') || hasProperty('startDate') || getProperty('significance', 0) > 20`

By default, events are attached to a single tree, unless the events have a `timeTreeRootId` (or its equivalent changed in config) property, in
 which case a tree rooted at the node with the specified ID will be used to attach the event.

Note that you can define multiple modules if desired, e.g.:
```
com.graphaware.runtime.enabled=true

com.graphaware.module.TT.1=com.graphaware.module.timetree.module.TimeTreeModuleBootstrapper
com.graphaware.module.TT.event=hasProperty('timestamp_enabled')
com.graphaware.module.TT.relationship=ENABLED_ON
com.graphaware.module.TT.autoAttach=true
com.graphaware.module.TT.timestamp=timestamp_enabled

com.graphaware.module.TS.2=com.graphaware.module.timetree.module.TimeTreeModuleBootstrapper
com.graphaware.module.TS.event=hasProperty('timestamp_disabled')
com.graphaware.module.TS.relationship=DISABLED_ON
com.graphaware.module.TS.autoAttach=true
com.graphaware.module.TS.timestamp=timestamp_disabled

...
```

### Java API

Java API has the same functionality as the rest API. Please refer to <a href="http://graphaware.com/site/timetree/latest/apidocs/" target="_blank">its Javadoc</a> (look at the `TimeTree` and `TimedEvents` interfaces).

License
-------

Copyright (c) 2013-2020 GraphAware

GraphAware is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with this program.
If not, see <http://www.gnu.org/licenses/>.
