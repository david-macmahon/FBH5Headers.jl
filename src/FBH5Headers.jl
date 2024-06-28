using CSV, Distributed, Dates
import Pkg

@info "starting $(now())"

##

# Add worker procs
ws = addprocs(collect(("blpc$i", :auto) for i in 0:3);
    exeflags="--project=$(Pkg.project().path)"
)
@info "added $(nworkers()) worker processes"

##

@info "defining functions everywhere"

@everywhere using HDF5, Blio

@everywhere function h5header(h5name)
    if isfile(h5name)
        try
            fbh = h5open(h5->Filterbank.Header(h5), h5name)
        catch
            fbh = Filterbank.Header()
        end
    else
        fbh = Filterbank.Header()
    end
    # Add h5name field to header
    fbh[:h5name] = h5name
    fbh
end

@everywhere function h5headers(h5name_channel, fbh_channel)
    # Loop through input files
    for h5name in h5name_channel
        if isempty(h5name)
            put!(fbh_channel, Filterbank.Header())
            break
        end
        put!(fbh_channel, h5header(h5name))
    end
    @debug "done"
    nothing
end

##

"""
    geth5names(fname) -> Vector of filenames

Get HDF5 names from a bldw query result log file named `fname`.
"""
function geth5names(fname)
    file = CSV.File(fname, skipto=3, delim='|',
                           stripwhitespace=true, select=["location"])
    map(file) do row
        replace(row[:location], "file://pd-datag/"=>"/datag/")
    end
end

##

# Create inter-proc channels and start worker procs

@info "creating remote channels to communicate with worker processes"
# Main process will put! h5names into h5name_channel.  Worker prcesses will take
# h5names from h5name_channel, read the header, and push the header to the
# fbh_channel ("fbh" for "Filterbank,Header").
h5name_channel = RemoteChannel(()->Channel{String}(Inf))
fbh_channel = RemoteChannel(()->Channel{Filterbank.Header}(Inf))

@info "spawning one task on each worker process"
futures = [@spawnat w h5headers(h5name_channel, fbh_channel) for w in ws]

##

@info "getting list of h5names"
@time h5names = geth5names("/home/jasjeev/c_band_data_0002.out")
#@time h5names = geth5names("/home/davidm/foo.out")
@info "got $(length(h5names)) file names"

@info "adding h5names to h5name_channel"
for h5name in h5names
    put!(h5name_channel, h5name)
end

# Put one empty string per worker to signal end of input
for i = 1:nworkers()
    put!(h5name_channel, "")
end

##

@info "waiting for workers to finish"
# Wait for the workers to finish.  No need to fetch since the workers don't
# return anything (other than through the fbh_channel)

waitforall(futures) = @time foreach(future->wait(future), futures)

#@profview waitforall(futures)
waitforall(futures)

##

remaining = nworkers()
@info "collecting results from $remaining workers..."

fbhs = Filterbank.Header[]
while remaining > 0
    fbh = take!(fbh_channel)
    if isempty(fbh)
        global remaining -= 1
    else
        push!(fbhs, fbh)
    end
end

@info "removing worker processes"
rmprocs(ws)

@info "got $(length(fbhs)) headers"

@info "done $(now())"
