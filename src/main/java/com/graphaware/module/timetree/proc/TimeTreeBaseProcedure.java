/*
 * Copyright (c) 2013-2019 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */
package com.graphaware.module.timetree.proc;

import java.util.Map;

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
