using CSV, Distributed, Dates, Sockets, ProgressBars, DataFrames
import Pkg

@info "starting $(now())"

##

@info "adding worker processes"
@time ws = addprocs([("blpc$i $(getaddrinfo("blpc$i.tenge.pvt"))", :auto) for i in 0:3];
    exeflags="--project=$(Pkg.project().path)"
)
@info "added $(nworkers()) worker processes"

##

@info "defining h5header function everywhere"

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

@info "getting list of h5names"
@time h5names = geth5names("/home/jasjeev/c_band_data_0002.out")
@info "got $(length(h5names)) h5names"

##

@info "parallel map h5names to headers using workers"
@time fbhs = pmap(h5header, ProgressBar(h5names); batch_size=50)
@info "got $(length(fbhs)) headers"

##

@info "creating DataFrame"
@time begin
    fbhsgood = filter(fbh->length(fbh)>1, fbhs)
    df = DataFrame(fbhsgood)
    # Sort column names, but put :h5name at the end
    select!(df, sort(names(df, Not(:h5name))), :h5name)
    # Sort rows by tstart then fch1
    sort!(df, [:tstart, :fch1])
end;

##

@info "removing worker processes"
rmprocs(ws)

# Any errors?
fbhsbad = filter(fbh->length(fbh)<=1, fbhs)
if length(fbhsbad) > 0
    @info "$(length(fbhsbad)) files had errors"
    println.(getindex.(fbhsbad, :h5name))
end

@info "done $(now())"
