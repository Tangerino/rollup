#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "cJSON.h"

int jsonToDouble(const char *json, const char *element, double *value) {
    int rc = ~0;
    cJSON *j;

    j = cJSON_Parse(json);
    if (!j) {
        printf("Error before: [%s]\n", cJSON_GetErrorPtr());
    } else {
        cJSON *obj = cJSON_GetObjectItem(j, element);
        if (obj) {
            *value = obj->valuedouble;
            rc = 0;
        }
    }
    cJSON_Delete (j);
    return rc;
}

int jsonToInt64(const char *json, const char *element, int64_t *value) {
    int rc = ~0;
    cJSON *j;

    j = cJSON_Parse(json);
    if (!j) {
        printf("Error before: [%s]\n", cJSON_GetErrorPtr());
    } else {
        cJSON *obj = cJSON_GetObjectItem(j, element);
        if (obj) {
            *value = obj->valueint;
            rc = 0;
        }
    }
    cJSON_Delete (j);
    return rc;
}

char *jsonToString(const char *json, const char *element) {
    cJSON *j;
    char *string = NULL;
    j = cJSON_Parse(json);
    if (!j) {
        printf("Error before: [%s]\n", cJSON_GetErrorPtr());
        return NULL;
    } else {
        cJSON *obj = cJSON_GetObjectItem(j, element);
        if (obj) {
            string = strdup (obj->valuestring);
        }
    }
    cJSON_Delete (j);
    return string;
}

int jsonGetInt64 (cJSON *j, const char *element, int64_t *value) {
    int rc = ~0;
    cJSON *obj = cJSON_GetObjectItem(j, element);
    if (obj) {
        *value = obj->valueint;
        rc = 0;
    }
    return rc;
}

int jsonGetDouble (cJSON *j, const char *element, double *value) {
    int rc = ~0;
    cJSON *obj = cJSON_GetObjectItem(j, element);
    if (obj) {
        *value = obj->valuedouble;
        rc = 0;
    }
    return rc;
}

int jsonGetString (cJSON *j, const char *element, char **value) {
    int rc = ~0;
    cJSON *obj = cJSON_GetObjectItem(j, element);
    if (obj) {
        *value = obj->valuestring;
        rc = 0;
    }
    return rc;
}

int jsonGetBoolean (cJSON *j, const char *element, int *value) {
    int rc = ~0;
    cJSON *obj = cJSON_GetObjectItem(j, element);
    if (obj) {
        *value = obj->type == cJSON_True;
        rc = 0;
    }
    return rc;
}

