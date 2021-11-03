package iot.meters.rest;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.smallrye.mutiny.Uni;
import iot.meters.streams.MeterAggregatedInteractiveQuery;
import iot.meters.streams.MeterUpdateInteractiveQuery;

@ApplicationScoped
@Path("/")
public class MeterResource {

    @Inject
    MeterUpdateInteractiveQuery meterUpdateInteractiveQuery;

    @Inject
    MeterAggregatedInteractiveQuery meterAggregatedInteractiveQuery;

    @GET
    @Path("/meter/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Response> getMeterUpdate(@PathParam("id") String id) {
        return meterUpdateInteractiveQuery.getMeterUpdate(id).onItem().transform(meterUpdate -> {
            if (meterUpdate == null || meterUpdate.isEmpty()) {
                return Response.status(Response.Status.NOT_FOUND.getStatusCode()).build();
            } else {
                return Response.ok(meterUpdate).build();
            }
        });
    }

    @GET
    @Path("/street/{street}")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Response> getMeterStatusPerStreet(@PathParam("street") String street) {
        return meterAggregatedInteractiveQuery.getMeterStatus(street).onItem().transform(meterStatus -> {
            if (meterStatus == null || meterStatus.isEmpty()) {
                return Response.status(Response.Status.NOT_FOUND.getStatusCode()).build();
            } else {
                return Response.ok(meterStatus).build();
            }
        });
    }
}
