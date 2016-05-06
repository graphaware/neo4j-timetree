/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.graphaware.module.timetree.proc;

import java.util.Map;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;

/**
 *
 * @author ale
 */
public class TimeTreeBaseProcedure {

    protected static final String PARAMETER_NAME_TIME = "time";
    protected static final String PARAMETER_NAME_RESOLUTION = "resolution";
    protected static final String PARAMETER_NAME_TIMEZONE = "timezone";
    protected static final String PARAMETER_NAME_START_TIME = "start";
    protected static final String PARAMETER_NAME_END_TIME = "end";
    protected static final String PARAMETER_NAME_ROOT = "root";
    protected static final String PARAMETER_NAME_RELATIONSHIP_TYPE = "relationshipType";
    protected static final String PARAMETER_NAME_NODE = "node";
    protected static final String PARAMETER_NAME_DIRECTION = "direction";
    protected static final String PARAMETER_NAME_RELATIONSHIP_TYPES = "relationshipTypes";
    protected static final String PARAMETER_NAME_INPUT = "input";
    protected static final String PARAMETER_NAME_INSTANT = "instant";
    protected static final String PARAMETER_NAME_INSTANTS = "instants";
    protected static final String PARAMETER_NAME_CREATE = "create";

    protected void checkTime(Map<String, Object> inputParams, String param) throws RuntimeException {
        try {
            Long time = (Long) inputParams.get(param);
            if (time == null) {
                throw new RuntimeException("No parameter " + param + " specified");
            }
        } catch (Exception ex) {
            throw new RuntimeException("Error getting " + param + " parameter", ex);
        }
    }

    protected void checkCreate(Map<String, Object> inputParams) throws RuntimeException {
        Object created = inputParams.getOrDefault(PARAMETER_NAME_CREATE, false);
        if (!(created instanceof Boolean) && !(created instanceof String)) {
            throw new RuntimeException("Wrong parameter value for 'create'. Admitted values are: 'true', 'false', 'yes', 'no', or no value at all (means false)");
        } else if (created instanceof String) {
            try {
                Boolean.parseBoolean((String) created);
            } catch (Exception ex) {
                throw new RuntimeException("Wrong parameter value for 'create': " + created + ""
                        + ". Admitted values are: 'true', 'false', 'yes', 'no', or no value at all (means false)", ex);
            }
        }
    }

    protected void checkIsMap(Object object) throws RuntimeException {
        if (!(object instanceof Map)) {
            throw new RuntimeException("Input parameter is not a map");
        }
    }

}
