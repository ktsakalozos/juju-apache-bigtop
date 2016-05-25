# Overview

This is the base layer for charms that wish to take advantage of
Apache's Bigtop framework for configuring and deploying services via
puppet. [Including this layer][building] gives you access to a Python
class called Bigtop, which contains various useful tools, including an
automatically run install routine.

# Usage

To create a charm using this base layer, first you must include it in
your `layer.yaml` file:

```yaml
includes: ['layer:apache-bigtop-base']
```

This will fetch the layer from [interfaces.juju.solutions][] and
incorporate it into your charm. To use the Bigtop class, import it,
then call its .render_site_yaml and .trigger_puppet routines in
sequence, like so:

```python
from charms.layer.apache_bigtop_base import Bigtop

# Setup arguments to pass to .render_site_yaml
hosts = {'myservice': <host>}
roles = ['myservice-worker', 'myservice-client']
override = {'myservice::somevalue': <somevalue>, ...}

# Trigger a puppet run
bigtop = Bigtop()
bigtop.render_site_yaml(hosts, roles, override)
bigtop.trigger_puppet()
```

This tells Bigtop to run puppet and install your service.

Bigtop can either infer which services you wish to install by
inspecting the "hosts" that you pass to .render_site_yaml, or it can
install the specific services that you name in "roles".

Of course, Bigtop needs to know how to deploy your service!

  * [List of "components" that can be inferred from hosts](https://github.com/apache/bigtop/blob/master/bigtop-deploy/puppet/hieradata/site.yaml)
  * [List of roles](https://github.com/apache/bigtop/blob/master/bigtop-deploy/puppet/manifests/cluster.pp)

# Reactive states

This layer will set the following states:

  * **`apache-bigtop-base.puppet_queued`** This indicates that puppet
      has been queued. The state will get removed fairly quickly, as
      the layer uses this state to trigger a puppet run.
  * **`bigtop.available`** This state is set after Bigtop.install has
      been run. At this point, you are free to invoke
      Bigtop.render_site_yaml.

# Layer configuration

Bigtop actually takes care of most of the configuration details for
you. You can specify overrides if needed, either in code, or in your
layer.yaml, and pass those in in an "overrides" dict when you call
Bigtop.render_site_yaml.

Depending on your service, you'll also need to tell Bigtop where to
find important hosts, by passing a dict of "hosts" to
Bigtop.render_site_yaml. You can use tools like the leadership layer
to populate your hosts.

# Actions

As of this writing, this layer does not define any actions.