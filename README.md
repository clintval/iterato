# iterato

[![Unit Tests](https://github.com/clintval/iterato/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/clintval/iterato/actions/workflows/unit-tests.yml)
[![Coverage Status](https://codecov.io/gh/clintval/iterato/branch/main/graph/badge.svg)](https://codecov.io/gh/clintval/iterato)
[![Language](https://img.shields.io/badge/language-scala-c22d40.svg)](https://www.scala-lang.org/)

Safe and completely lazy parallel iteration in Scala.

![Fish Lake, Washington](.github/img/cover.jpg)

#### Examples

```scala
import scala.concurrent.ExecutionContext.Implicits.global

Iterator(1, 2, 3).parMap(_ + 5).toSeq shouldBe Seq(6, 7, 8)
```

#### Features

- Evaluate an expression in parallel over an iterator with complete laziness
- Prevent excessive back pressure by blocking on a bounded capacity queue
- All exceptions in the input iterator, or applied function, are raised
- Unlike [`iterata`](https://github.com/tim-group/iterata), maximize CPU utilization by avoiding data chunking
- Unlike `Future.traverse`, do not accumulate results before emitting them

#### If Mill is your build tool

```scala
ivyDeps ++ Agg(ivy"io.cvbio.collection::iterato::0.0.1")
```

#### If SBT is your build tool

```scala
libraryDependencies += "io.cvbio.collection" %% "iterato" % "0.0.1"
```
