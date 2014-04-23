#require './CsvMachine.rb'
#run CsvMachine

require 'rubygems'
require 'sinatra'

set :env, :production
disable :run

require "./CsvMachine.rb"

run CsvMachine
#run Sinatra::Base
