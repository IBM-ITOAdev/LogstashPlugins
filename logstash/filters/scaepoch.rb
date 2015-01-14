# encoding: utf-8

############################################                                    
#                                                                               
# SCAEPOCH                               
#                                                                               
# Logstash mediation filter for SCAPI
#                                
# Extracts timestamp information using supplied format pattern 
# and assigns derived  epoch time to a designated field
#                                                                               
# Version 130114.1 Robert Mckeown                                               
#                                                                               
############################################     

require "logstash/filters/base"
require "logstash/namespace"
require "logstash/environment"
require "date"

class LogStash::Filters::SCAEpoch < LogStash::Filters::Base

  config_name "scaepoch"
  milestone 1

  config :input_field, :validate => :string, :required => true
  config :output_field, :validate => :string, :required => true
  config :pattern, :validate => :string, :required => true

  public
  def initialize(config = {})
    super

  end # def initialize

  public
  def register
 
  end # def register

  public
  def filter(event)

    begin

      if event[@input_field] != "SCAWindowMarker" # silently filter this out-of-band event
             event[@output_field] = 
          DateTime.strptime(event[@input_field],@pattern).to_time.to_i.to_s
      end

    rescue
        @logger.warn("Exception processing " + event[@input_field] + " with pattern " + @pattern)
  
    end

  end # def filter

end # class LogStash::Filters::SCAEpoch
