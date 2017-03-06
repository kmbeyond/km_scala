/**
  * Created by kiran on 2/28/17.
  */
object ScalaImplicitTest {


  implicit def extensions[F[_], A](m: F[A]) = new {
    def foundIt() = ()
  }

  case class Foo[T](x: T)

  val f1: Foo[Int] = Foo(42)
  f1.foundIt()
  // Implicit is able to match and infer
  type FooType[T] = Foo[T]
  val f2: FooType[Int] = Foo(42)
  f2.foundIt()

  // Implicit is able to match and infer

  case class Foo2[T, V](x: T, y: V)

  val f3: Foo2[Int, Double] = Foo2(4, 2.0)
  // f3.foundIt()  // Doesn't work of course
  type Foo2Type[T] = Foo2[T, Double]
  val f4: Foo2Type[Int] = Foo2(42, 2.0)
  // f4.foundIt() // Why doesn't this work ...
  extensions[Foo2Type, Int](f4).foundIt() //  if this works?


}