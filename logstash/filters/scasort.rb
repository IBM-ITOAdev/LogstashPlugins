
############################################                                    
#                                                                               
# SCASORT                               
#                                                                               
# Logstash mediation filter for SCAPI - simple sort                                
#                                                                               
# Version 081214.2 Robert Mckeown                                               
#                                                                               
############################################     



require "logstash/filters/base"
require "logstash/namespace"
require "logstash/environment"

class LogStash::Filters::SCASORT < LogStash::Filters::Base

  config_name "scasort"
  milestone 1

  config :sort_field, :validate => :string, :required => false, :default => "timestamp"
  config :punctuation_field, :validate => :string, :required => false, :default => "SCA:WindowMarker"


  public
  def initialize(config = {})
    super
    @bufferedEvents = []
  end # def initialize

  public
  def register
 
  end # def register

  public
  def filter(event)
    return unless filter?(event)

    if event['message'] == "SCAWindowMarker"

      # Have all the data, sort and output
      @bufferedEvents.sort! { |a,b| a[@sort_field] <=> b[@sort_field] }
      @bufferedEvents.each do | origEvent |

        clonedEvent = origEvent.clone
        clonedEvent['sorted']="TRUE"
        filter_matched(clonedEvent)
        yield clonedEvent
      end
      @bufferedEvents.clear
      # ensure that the window marker comes out at the end                                                                                              
 #     newWindowMarker = event.clone
 #     newWindowMarker.uncancel
      # cancel the original window marker                                                                                                               
     else
      event.cancel
      @bufferedEvents.push(event)
    end

  end # def filter

end # class LogStash::Filters::SCABMCPREP
