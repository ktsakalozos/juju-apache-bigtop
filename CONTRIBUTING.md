## Contributing

This charm has been built on top of the Apache Bigtop project. Its source code lives in the main Bigtop repository on Github: https://github.com/apache/bigtop

Bug fixes, new features and other contributions are welcome, and encouraged! Since this is a joint project between Canonical Charmers and the Apache Bigtop community, it can be a little tricky to get the code submitted to all the right places, and +1ed by the right people, however.

What follows are the best practices to follow when submitting a new feature or bugfix, in order to ensure quick review and feedback:

1. Make a ticket to track the work that you're doing in Bigtop's Jira: https://issues.apache.org/jira/browse/BIGTOP/
2. Make a fork of the Bigtop project in your own Github account.
3. You'll find code for this charm in ``bigtop-packages/src/charms/<charm-name>`` in your copy of the repo. Make your changes there.
4. Submit your changes by creating a pull request from your fork to the main bigtop repository. Apache's Jira will automatically attach your pull request to your Jira ticket if you follow the formatting guidelines below.
5. To make your charm publicly available for others to use, build it, and submit it to the [Charm Store](https://jujucharms.com/docs/2.0/authors-charm-store).

Please feel free to ping us on freenode#juju, or email juju@lists.canonical.com if you have any questions. Happy coding!

## Pull Request Formatting

Apache's Jira automatically associates Jira tickets and Github pull requests, provided you format the title of your pull request as follows:

```BIGTOP-<ticket number> <ticket title>```

``<ticket_number>`` should be the number of your Jira ticket, and ``<ticket title>`` should be a string that exactly matches the title of your Jira ticket.
