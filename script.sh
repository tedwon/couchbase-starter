#!/bin/bash

exec scala "$0" "$@"
!#
object HelloWorld extends App {
    println("Hello, scala script!")
}
HelloWorld.main(args)