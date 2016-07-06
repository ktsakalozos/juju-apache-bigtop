## Overview

This is the base layer for charms that wish to take advantage of
Apache's Bigtop framework for configuring and deploying services via
puppet. [Including this layer][building] gives you access to a Python
class called Bigtop, which contains, among other things, tools for
triggering a puppet run.

## Usage

To create a charm using this base layer, first you must include it in
your `layer.yaml` file:

```yaml
includes: ['layer:apache-bigtop-base']
```

This will fetch the layer from [interfaces.juju.solutions][] and
incorporate it into your charm. To use the Bigtop class, import it,
then call its .render_site_yaml and .trigger_puppet routines in
sequence, like so (you might notice that Bigtop also has an .install
routine -- this is run automatically in the reactive handlers of this
layer; you don't need to call it yourself):

```python
from charms.layer.apache_bigtop_base import Bigtop

# Setup arguments to pass to .render_site_yaml
hosts = {'some_bigtop_service': <host>}
roles = ['some_bigtop_service_nodetype0', 'some_bigtop_service_nodetype1']
override = {'some_bigtop_service::somekey': <somevalue>, ...}

# Trigger a puppet run
bigtop = Bigtop()
bigtop.render_site_yaml(hosts, roles, override)
bigtop.trigger_puppet()
```

This tells Bigtop to run puppet and install your service.

How does Bigtop know what to install? You tell it what to install by
passing a list of "roles" to .render_site_yaml. You may wish to
consult [this list of valid
roles](https://github.com/apache/bigtop/blob/master/bigtop-deploy/puppet/manifests/cluster.pp)
to see what is available.

*Note*: Bigtop is also able to generate a list of roles from a
"component". The Bigtop class is theoretically able to infer
components from the "hosts" dict that you pass to
.render_site_yaml. As of this writing, this code path is not well
tested, however, so you may want to specify roles explicitly. [List of
valid
components](https://github.com/apache/bigtop/blob/master/bigtop-deploy/puppet/hieradata/site.yaml)

## Reactive states

This layer will set the following states:

  * **`bigtop.available`** This state is set after Bigtop.install has
      been run. At this point, you are free to invoke
      Bigtop.render_site_yaml.

## Layer configuration

Bigtop actually takes care of most of the configuration details for
you. You can specify overrides if needed, either in code, or in your
layer.yaml, and pass those in in an "overrides" dict when you call
Bigtop.render_site_yaml.

Depending on your service, you'll also need to tell Bigtop where to
find important hosts, by passing a dict of "hosts" to
Bigtop.render_site_yaml.

The leadership layer may come in handy when it comes to populating
that hosts dict.

## Actions

As of this writing, this layer does not define any actions.

## Contact Information

- <bigdata@lists.ubuntu.com>

## Unit Tests

To run unit tests for this layer, simple change to the root directory
of the layer in a terminal, and run "tox". To tweak settings, such as
making the tests more or less verbose, edit tox.ini.

## Help

- [Juju mailing list](https://lists.ubuntu.com/mailman/listinfo/juju)
- [Juju community](https://jujucharms.com/community)
