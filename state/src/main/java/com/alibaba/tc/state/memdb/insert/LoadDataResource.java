package com.alibaba.tc.state.memdb.insert;

import com.alibaba.sdb.metadata.CatalogManager;
import com.facebook.airlift.log.Logger;
import com.google.common.base.Throwables;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

@Path("/v1/load")
public class LoadDataResource
{
    private static final Logger log = Logger.get(com.alibaba.sdb.server.protocol.DirectInsert.class);
    private final CatalogManager catalogManager;

    @Inject
    public LoadDataResource(CatalogManager catalogManager)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
    }

    @POST
    @Path("{catalog}/{schema}/{table}")
    @Consumes(TEXT_PLAIN)
    @Produces(TEXT_PLAIN)
    public Response createQuery(
            @PathParam("catalog") String catalog,
            @PathParam("schema") String schema,
            @PathParam("table") String table,
            String text)
    {
        try {
            int rows = new com.alibaba.sdb.server.protocol.DirectInsert(catalogManager, catalog, schema, table, text).insert();
            return Response.ok(rows, TEXT_PLAIN).build();
        }
        catch (Throwable t) {
            log.error(t);
            return Response.ok(Throwables.getStackTraceAsString(t), TEXT_PLAIN).build();
        }
    }
}
