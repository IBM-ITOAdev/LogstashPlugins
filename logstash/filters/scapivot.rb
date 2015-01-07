# encoding: utf-8

############################################                                    
#                                                                               
# SCAPIVOT                               
#                                                                               
# Logstash mediation filter for SCAPI - pivoting                                
#                                                                               
# Version 261114.1 Robert Mckeown                                               
#                                                                               
############################################     

require "logstash/filters/base"
require "logstash/namespace"
require "logstash/environment"
require "set"


#
# This filter pivots sets of multiple events to single ones aka  'Skinny->Wide'
#
#
# The config looks like this:
#
#     filter {
#       scapivot {
#         window_column =>  ["timestamp"]
#         fixed_columns =>  ["timestamp","host","device","group"]
#         target_column =>  ["metric"]
#         value_column  =>  ["value"]
#         flush_interval => 60
#       }
#     }        

class LogStash::Filters::SCAPivot < LogStash::Filters::Base

  config_name "scapivot"
  milestone 1

  config :window_column, :validate => :string, :default => "timestamp"
  config :fixed_columns, :validate => :array
  config :target_column, :validate => :string
  config :value_column, :validate => :string
#  config :flush_interval, :validate => :number, :default => 60

  public
  def initialize(config = {})
    super

    @logger.debug("** SCAPivot initialize")

    @threadsafe = false

    # store the current set / window of events in @pending
    @pending = Set.new
    
    @currentWindowCount = 0

    # track the current timestamp
    @currentTimestamp

   end # def initialize

  public
  def register
 
  end # def register

  public
  def filter(event)
    return unless filter?(event)
     @logger.debug("*** SCAPivot entering filter")
     @logger.debug(event.to_hash)

     # Catch punctuation

     if event["message"] == "SCAWindowMarker"

        if @pending.length > 0 
         pivoted = pivot(@pending, @target_column, @value_column, @fixed_columns)
         pivoted.each do | newEvent |
           yield newEvent
         end

         # ensure that the window marker comes out at the end
         newWindowMarker = event.clone
         # cancel the original window marker
         event.cancel
         yield newWindowMarker

         @pending.clear
         @currentTimestamp = nil

        end
        # In any case, the punctuation falls through and is output
     # not punctuation, normal processing
     else
     if @currentTimestamp.nil? 
         @logger.debug("currentTimestamp is nil. Appending and setting to " + event[@window_column])
         event.cancel
         addToPending(event.clone)
         @currentTimestamp = event[@window_column]
         @currentWindowCount = @currentWindowCount + 1 
       elsif event[@window_column] == @currentTimestamp
         @logger.debug("Match with current timestamp " + @currentTimestamp) 
         event.cancel
         addToPending(event.clone)
       else
         @logger.debug("No match between existing timestamp " + @currentTimestamp + "/" + event[@window_column] ) 
         # New 'timestamp'
         # Need to flush out existing
         @logger.debug("*** SCAPivot Pivoting window " + @currentWindowCount.to_s + " Size = " + @pending.size.to_s )

         pivoted = pivot(@pending, @target_column, @value_column, @fixed_columns)

         @logger.debug("*** SCAPivot Pivoting finished - pivoted size = " + pivoted.size.to_s )

         pivoted.each do | newEvent |
           yield newEvent
         end

         @pending.clear
         event.cancel 
         addToPending(event.clone)
         @currentTimestamp = event[@window_column] 
         @currentWindowCount = @currentWindowCount + 1 
 
       end 
     end 
     
     @logger.debug("*** SCAPivot leaving filter")

  end # def filter


private
def addToPending(event)

  @pending.add(event)
  @timeOfLastAdd = Time.now.to_i
  
end

private
def pivot(events, targetColumn, valueColumn, fixedColumns)

  @logger.debug("*** SCAPivot in pivot \n")
  @logger.debug("*** events.size=" + events.size.to_s + "\n")
  @logger.debug("*** fixedColumns=" + fixedColumns.to_s + "\n")

  uniqueFixedColumnAssocs = uniqueRowsForFixedColumns(events,fixedColumns)

  @logger.debug("*** uniqueFixedColumnAssocs.size=" + uniqueFixedColumnAssocs.size.to_s + "\n")

  # uniqueGroups is Array of Arrays
  uniqueGroups =
    uniqueFixedColumnAssocs.collect {
      | keyColumnAssoc |
    events.select {
        | event |
      matchesFixedColumn?(event,keyColumnAssoc)
    }
  }

  # First hash                                                                     
  pivotedEvents =
    uniqueGroups.collect {
    | group |

    # Make a new event, plucking fixed column values from the first entry                                                                
    newEvent = newEventCorrespondingToFixedColumns(group.first,fixedColumns)

    group.each do | event |

      if (event[targetColumn].nil? || event[valueColumn].nil?)   
          @logger.debug("NilEventTargetOrValueColumn - Dropping Event" + event.to_json.to_s  ) 
          # shouldn't really have an event reaching here without a targetColumn value.
          # This is indicative of processing problems upstream
          # Just to keep things going, replace the nil with an empty string
      else
          newEvent[event[targetColumn]]=event[valueColumn]
      end
    end
    newEvent
  }

  return pivotedEvents
end

private
def newEventCorrespondingToFixedColumns( event, fixedColumns )

#  newHashEvent = Hash.new
   newHashEvent = LogStash::Event.new("message" => "Pivoted from originals") 

  fixedColumns.each do | key |
 
    if (event[key].nil?) 
      newHashEvent[key] = "" # stick an empty string there, just in case 
    else
      newHashEvent[key] = event[key]
    end
  end

  return newHashEvent

end

private
def matchesFixedColumn?(event,keyColumnAssoc)

  keyColumnAssoc.each {

    | colKey,colValue |
    if ( !(event[colKey] == colValue ))
      return false
    end
  }

  return true

end

private
def uniqueRowsForFixedColumns(events,fixedColumns)

  hashForFixedColumns = events.collect {
   |event|
    h = Hash.new
    fixedColumns.collect {
       |key|
       h[key] = event[key]
    }
    h
  }

  @logger.debug("hashForFixedColumns=" + hashForFixedColumns.size.to_s)

  # Use Set's uniqueness to get the unique set                                                                                                            
  uniqueAssocs = Set.new
  hashForFixedColumns.each do | assoc |
    uniqueAssocs.add(assoc)
  end

  @logger.debug("uniqAssocs=" + uniqueAssocs.size.to_s)

  return uniqueAssocs

end


end # class LogStash::Filters::SCAPivot
