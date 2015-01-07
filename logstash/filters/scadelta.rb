# encoding: utf-8

############################################                                    
#                                                                               
# SCADELTA                               
#                                                                               
# Logstash mediation filter for SCAPI - simple delta                                
#                                                                               
# Version 081214.2 Robert Mckeown                                               
#                                                                               
############################################     

require "logstash/filters/base"
require "logstash/namespace"
require "logstash/environment"
require "set"

class LogStash::Filters::SCADelta < LogStash::Filters::Base

  config_name "scadelta"
  milestone 1

  config :input_field, :validate => :string
  config :output_field, :validate => :string

  public
  def initialize(config = {})
    super

  end # def initialize

  public
  def register
 
  end # def register

  public
  def filter(event)

     if @lastEvent.nil? 
       @logger.debug("last event is nil. Remembering it")
     else
       event[@output_field] = (event[@input_field].to_f) - (@lastEvent[@input_field]).to_f       
     end

     # For next time
     @lastEvent = event
       
     @logger.debug("*** SCADelta leaving filter")

  end # def filter

end # class LogStash::Filters::SCADelta
